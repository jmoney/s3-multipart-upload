// Write a program that multipart uploads multiple files to s3 using goroutines and channels.
// The program should take a directory as input and upload all files concurrently.
// The program should output the total number of files processed and elapsed time.
// The program should be able to handle errors gracefully.
// The program should be able to handle large files.

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
	PART_SIZE = 5 * 1024 * 1024 // 5MB per part

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
		checkpoint.Write([]byte(fmt.Sprintf("%s,%d\n", *uploadId, partNumber)))

		numParts++
	}

	fmt.Printf("Successfully uploaded %s to %s\n", fileName, bucketName)
	return nil
}

func findOrCreateMultipartUpload(svc *s3.S3, bucketName, dirName, fileName string) (*string, int64, *os.File) {
	sum := sha256.Sum256([]byte(fileName))
	checkpointName := filepath.Join(*checkpointDir, fmt.Sprintf("%x", sum))
	checkpoint, err := os.OpenFile(checkpointName, os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatal(err)
		} else {
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
		must(strconv.ParseInt(strings.Split(string(buf), ",")[1], 10, 64)),
		checkpoint
}

func must[T any](value T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return value
}
