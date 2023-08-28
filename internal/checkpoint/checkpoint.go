package checkpoint

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jmoney/s3-multipart-upload/internal/util"
)

var (
	ilog *log.Logger = log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile)
	wlog *log.Logger = log.New(os.Stdout, "[WARN] ", log.Ldate|log.Ltime|log.Lshortfile)
	elog *log.Logger = log.New(os.Stderr, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
)

type CompletedPart struct {
	ETag       *string
	PartNumber *int64
	Size       *int64
}

type Checkpoint interface {
	New(fileName string)
	Save(buf *[]byte) error
	Complete() error
	Seek(file *os.FileInfo) error
	Close()
}

type LocalCheckpoint struct {
	uploadID       *string
	parts          []CompletedPart
	checkpointFile *os.File
	svc            *s3.S3
	bucket         string
	key            string
}

func (cp *LocalCheckpoint) Close() {
	cp.checkpointFile.Close()
}

func New(bucketName, fileName, checkpointDir string) LocalCheckpoint {
	cp := LocalCheckpoint{}
	cp.parts = make([]CompletedPart, 0)
	cp.svc = s3.New(session.Must(session.NewSession()))
	cp.bucket = bucketName
	cp.key = fileName

	sum := sha256.Sum256([]byte(fileName))
	checkpointName := filepath.Join(checkpointDir, fmt.Sprintf("%x", sum))
	dir, err := os.Stat(checkpointDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.Mkdir(checkpointDir, 0755)
			if err != nil {
				elog.Fatal(err)
			}
		} else {
			elog.Fatal(err)
		}
	} else if !dir.IsDir() {
		elog.Fatalf(".checkpoints is not a directory")
	}
	checkpointFile, err := os.OpenFile(checkpointName, os.O_RDWR, 0755)
	cp.checkpointFile = checkpointFile
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			elog.Fatal(err)
		}

		// The error was the file did not exist so create the file and save the handle
		cp.checkpointFile = util.Must(os.Create(checkpointName))
		multipartUloadOutput, err := cp.svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(strings.TrimPrefix(fileName, "/")),
		})
		if err != nil {
			elog.Fatal(err)
		}
		cp.uploadID = multipartUloadOutput.UploadId
		cp.checkpointFile.Write([]byte(fmt.Sprintf("%s\n", *multipartUloadOutput.UploadId)))
		cp.checkpointFile.Sync()
		return cp
	}

	if cp.checkpointFile == nil || util.Must(cp.checkpointFile.Stat()).Size() != 0 {
		// read checkpoint file
		fileScanner := bufio.NewScanner(cp.checkpointFile)
		fileScanner.Split(bufio.ScanLines)
		fileScanner.Scan()
		cp.uploadID = aws.String(fileScanner.Text())
		for fileScanner.Scan() {
			line := strings.Split(fileScanner.Text(), ",")
			cp.parts = append(cp.parts, CompletedPart{
				ETag:       aws.String(line[0]),
				PartNumber: aws.Int64(util.Must(strconv.ParseInt(line[1], 10, 64))),
				Size:       aws.Int64(util.Must(strconv.ParseInt(line[2], 10, 64))),
			})
		}
	}

	return cp
}

func (cp *LocalCheckpoint) Save(buf *[]byte) error {
	partNumber := int64(len(cp.parts) + 1)
	ilog.Printf("Uploading part %d of %s\n", partNumber, cp.key)
	upload, err := cp.svc.UploadPart(&s3.UploadPartInput{
		Body:       bytes.NewReader(*buf),
		Bucket:     aws.String(cp.bucket),
		Key:        aws.String(strings.TrimPrefix(cp.key, "/")),
		PartNumber: aws.Int64(partNumber),
		UploadId:   cp.uploadID,
	})

	if err != nil {
		return err
	}

	completedPart := CompletedPart{
		ETag:       upload.ETag,
		PartNumber: &partNumber,
		Size:       aws.Int64(int64(len(*buf))),
	}

	cp.parts = append(cp.parts, completedPart)
	_, err = cp.checkpointFile.Write([]byte(fmt.Sprintf("%s,%d,%d\n", *completedPart.ETag, *completedPart.PartNumber, *completedPart.Size)))
	if err != nil {
		return err
	}
	cp.checkpointFile.Sync()
	return nil
}

func (cp *LocalCheckpoint) Complete() error {
	completedMultipartUploadInput := s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(cp.bucket),
		Key:      aws.String(strings.TrimPrefix(cp.key, "/")),
		UploadId: cp.uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: toS3CompletedParts(cp.parts),
		},
	}
	_, err := cp.svc.CompleteMultipartUpload(&completedMultipartUploadInput)

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchUpload:
			wlog.Println(s3.ErrCodeNoSuchUpload)
			return nil
		default:
			elog.Printf("Error completed upload %v\n", completedMultipartUploadInput)
			return err
		}
	} else {
		ilog.Printf("Successfully uploaded %s to %s\n", cp.key, cp.bucket)
	}

	return nil
}

func (cp *LocalCheckpoint) Seek(file *os.File) error {
	seekOffset := int64(0)
	for _, part := range cp.parts {
		seekOffset += *part.Size
	}

	_, err := file.Seek(seekOffset, io.SeekStart)
	return err
}

// Function that takes a slice of CompletedPart and returns a slice of s3.CompletedPart
func toS3CompletedParts(cps []CompletedPart) []*s3.CompletedPart {
	s3cps := make([]*s3.CompletedPart, 0)
	for _, cp := range cps {
		s3cps = append(s3cps, toS3CompletedPart(cp))
	}
	return s3cps
}

// Function that takes a CompletedPart and returns a s3.CompletedPart
func toS3CompletedPart(cp CompletedPart) *s3.CompletedPart {
	return &s3.CompletedPart{
		ETag:       cp.ETag,
		PartNumber: cp.PartNumber,
	}
}
