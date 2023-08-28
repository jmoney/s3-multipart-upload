package main

import (
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jmoney/s3-multipart-upload/internal/checkpoint"
)

type Part struct {
	ETag       *string
	PartNumber *int64
	Size       *int64
}

var (
	DEFAULT_PART_SIZE = int64(100 * 1024 * 1024) // 100MB per part

	bucketName    = flag.String("bucket", "", "Bucket name to upload to")
	dirName       = flag.String("dir", "", "Directory name containing the files to upload")
	checkpointDir = flag.String("checkpoint", ".checkpoints", "Checkpoint directory")
	partSize      = flag.Int64("partsize", DEFAULT_PART_SIZE, "Part size in bytes")

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
	if *bucketName == "" || *dirName == "" {
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
					Error.Panic(err)
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

	local := checkpoint.New(bucketName, filepath.Join(dirName, fileName), *checkpointDir)
	defer local.Close()
	local.Seek(file)

	for {
		buffer := make([]byte, *partSize)
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		err = local.Save(&buffer)
		if err != nil {
			Error.Fatal(err)
		}
	}

	return local.Complete()
}
