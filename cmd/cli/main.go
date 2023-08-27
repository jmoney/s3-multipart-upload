package main

import (
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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	PART_SIZE = 100 * 1024 * 1024 // 100MB per part

	bucketName    = flag.String("bucket", "", "Bucket name")
	dirName       = flag.String("dir", "", "Directory name")
	checkpointDir = flag.String("checkpoint", "", "Checkpoint directory")
	region        = flag.String("region", "", "Region name")
)

func main() {
	flag.Parse()
	if *bucketName == "" || *dirName == "" || *region == "" || *checkpointDir == "" {
		flag.Usage()
		os.Exit(1)
	}
	start := time.Now()
	files, err := os.ReadDir(*dirName)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, file := range files {
		if !file.IsDir() {
			wg.Add(1)
			go func(fileName string) {
				defer wg.Done()
				err := uploadFile(*bucketName, *dirName, fileName, *region)
				if err != nil {
					log.Println(err)
				}
			}(file.Name())
		}
	}
	wg.Wait()
	fmt.Printf("Total time taken: %v\n", time.Since(start))
}

func uploadFile(bucketName, dirName, fileName, region string) error {
	file, err := os.Open(filepath.Join(dirName, fileName))
	if err != nil {
		return err
	}
	defer file.Close()
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return err
	}

	svc := s3.New(sess)
	// upload parts to s3
	var partNumber int64
	var partSize int64 = int64(PART_SIZE)
	var numParts int
	uploadId, partNumber, checkpoint := findOrCreateMultipartUpload(svc, bucketName, dirName, fileName)
	defer checkpoint.Close()

	// seek to the last checkpoint
	file.Seek(partNumber*partSize, io.SeekStart)

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

		log.Printf("Uploading part %d of %s\n", partNumber, filepath.Join(dirName, fileName))
		_, err = svc.UploadPart(&s3.UploadPartInput{
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
		checkpoint.Seek(0, io.SeekStart)
		checkpoint.Write([]byte(fmt.Sprintf("%s,%d\n", *uploadId, partNumber)))
		checkpoint.Sync()

		numParts++
	}

	svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(strings.TrimPrefix(filepath.Join(dirName, fileName), "/")),
		UploadId: uploadId,
	})

	fmt.Printf("Successfully uploaded %s to %s\n", fileName, bucketName)
	return nil
}

func findOrCreateMultipartUpload(svc *s3.S3, bucketName, dirName, fileName string) (*string, int64, *os.File) {
	sum := sha256.Sum256([]byte(filepath.Join(dirName, fileName)))
	checkpointName := filepath.Join(*checkpointDir, fmt.Sprintf("%x", sum))
	checkpoint, err := os.OpenFile(checkpointName, os.O_RDWR, 0755)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatal(err)
		} else if errors.Is(err, os.ErrNotExist) || checkpoint == nil || must(checkpoint.Stat()).Size() == 0 {
			multipartUloadOutput, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(strings.TrimPrefix(filepath.Join(dirName, fileName), "/")),
			})
			if err != nil {
				log.Fatal(err)
			}

			checkpointFile, err := os.Create(checkpointName)
			if err != nil {
				log.Fatal(err)
			}
			return multipartUloadOutput.UploadId, int64(0), checkpointFile
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	// read checkpoint file
	buf := make([]byte, must(checkpoint.Stat()).Size())
	_, err = checkpoint.Read(buf)
	if err != nil {
		log.Fatal(err)
	}

	return aws.String(strings.Split(string(buf), ",")[0]),
		must(strconv.ParseInt(strings.TrimSpace(strings.Split(string(buf), ",")[1]), 10, 64)),
		checkpoint
}

// Function that take a tuple of a value and error.  If error is not nil, it exits and logs. Else, returns the value.
func must[T any](value T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return value
}
