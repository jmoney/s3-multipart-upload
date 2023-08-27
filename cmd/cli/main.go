package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Part struct {
	ETag       *string
	PartNumber *int64
	Size       *int64
}

var (
	PART_SIZE = 100 * 1024 * 1024 // 100MB per part

	bucketName    = flag.String("bucket", "", "Bucket name")
	dirName       = flag.String("dir", "", "Directory name")
	checkpointDir = flag.String("checkpoint", "", "Checkpoint directory")

	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
)

func init() {
	Info = log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile)
	Warn = log.New(os.Stdout, "[WARN] ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(os.Stdout, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	flag.Parse()
	if *bucketName == "" || *dirName == "" || *checkpointDir == "" {
		flag.Usage()
		os.Exit(1)
	}
	start := time.Now()
	files, err := os.ReadDir(*dirName)
	if err != nil {
		Error.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, file := range files {
		if !file.IsDir() {
			wg.Add(1)
			go func(fileName string) {
				defer wg.Done()
				err := uploadFile(*bucketName, *dirName, fileName)
				if err != nil {
					Error.Println(err)
				}
			}(file.Name())
		}
	}
	wg.Wait()
	Info.Printf("Total time taken: %v\n", time.Since(start))
}

func uploadFile(bucketName, dirName, fileName string) error {
	file, err := os.Open(filepath.Join(dirName, fileName))
	if err != nil {
		return err
	}
	defer file.Close()
	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	svc := s3.New(sess)
	// upload parts to s3
	var partNumber int64 = 0
	var partSize int64 = int64(PART_SIZE)
	var numParts int
	uploadId, completedParts, checkpoint := findOrCreateMultipartUpload(svc, bucketName, dirName, fileName)
	defer checkpoint.Close()

	if len(completedParts) > 0 {
		// seek the file to the end of the last completed part
		offset := int64(0)
		for _, part := range completedParts {
			offset += *part.Size
		}

		file.Seek(offset, io.SeekStart)
		partNumber = *completedParts[len(completedParts)-1].PartNumber
	}

	for {
		partNumber++
		buffer := make([]byte, partSize)
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		Info.Printf("Uploading part %d of %s\n", partNumber, filepath.Join(dirName, fileName))
		upload, err := svc.UploadPart(&s3.UploadPartInput{
			Body:       bytes.NewReader(buffer),
			Bucket:     aws.String(bucketName),
			Key:        aws.String(strings.TrimPrefix(filepath.Join(dirName, fileName), "/")),
			PartNumber: aws.Int64(partNumber),
			UploadId:   uploadId,
		})

		if err != nil {
			return err
		}

		// Now, we need to checkpoint parts to a local file to we can restart if network becomes unstable
		// We can use a local file to store the checkpoint
		checkpoint.Write([]byte(fmt.Sprintf("%s,%d,%d\n", *upload.ETag, partNumber, partSize)))
		checkpoint.Sync()

		numParts++
	}

	var s3Parts []*s3.CompletedPart
	for _, part := range completedParts {
		s3Parts = append(s3Parts, &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	_, err = svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(strings.TrimPrefix(filepath.Join(dirName, fileName), "/")),
		UploadId: uploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: s3Parts,
		},
	})

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchUpload:
			Warn.Println(s3.ErrCodeNoSuchUpload)
		default:
			Error.Fatal(aerr.Error())
		}
	} else {
		Info.Printf("Successfully uploaded %s to %s\n", fileName, bucketName)
	}

	return nil
}

func findOrCreateMultipartUpload(svc *s3.S3, bucketName, dirName, fileName string) (*string, []Part, *os.File) {
	sum := sha256.Sum256([]byte(filepath.Join(dirName, fileName)))
	checkpointName := filepath.Join(*checkpointDir, fmt.Sprintf("%x", sum))
	checkpoint, err := os.OpenFile(checkpointName, os.O_RDWR, 0755)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			Error.Fatal(err)
		} else if errors.Is(err, os.ErrNotExist) || checkpoint == nil || must(checkpoint.Stat()).Size() == 0 {
			multipartUloadOutput, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(strings.TrimPrefix(filepath.Join(dirName, fileName), "/")),
			})
			if err != nil {
				Error.Fatal(err)
			}

			checkpointFile, err := os.Create(checkpointName)
			if err != nil {
				Error.Fatal(err)
			}
			checkpointFile.Write([]byte(fmt.Sprintf("%s\n", *multipartUloadOutput.UploadId)))
			return multipartUloadOutput.UploadId, []Part{}, checkpointFile
		}
	}

	if err != nil {
		Error.Fatal(err)
	}

	// read checkpoint file
	fileScanner := bufio.NewScanner(checkpoint)
	fileScanner.Split(bufio.ScanLines)
	fileScanner.Scan()
	uploadId := fileScanner.Text()
	var completedParts []Part
	for fileScanner.Scan() {
		line := strings.Split(fileScanner.Text(), ",")
		completedParts = append(completedParts, Part{
			ETag:       aws.String(line[0]),
			PartNumber: aws.Int64(must(strconv.ParseInt(line[1], 10, 64))),
			Size:       aws.Int64(must(strconv.ParseInt(line[2], 10, 64))),
		})
	}

	return aws.String(uploadId),
		completedParts,
		checkpoint
}

// Function that take a tuple of a value and error.  If error is not nil, it exits and logs. Else, returns the value.
func must[T any](value T, err error) T {
	if err != nil {
		Error.Fatal(err)
	}
	return value
}
