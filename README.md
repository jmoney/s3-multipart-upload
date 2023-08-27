# s3-multipart-upload

Upload files in a directory to s3 via multipart uploads.  The uploaded parts are checkpointed to a local file for restarting purposes.

## Overview

| Arguemment | Description |
| --- | --- |
| bucket | The bucket name to multipart upload too. |
| checkpoint | The directory to store the checkpoint files. |
| dir | The directory of all the files to multipart upload with. This directory will be mirrored into s3. |

## Installation

```bash
brew tap jmoney/aws
brew install s3-multipart-upload
```

## Run Locally

```bash
go run cmd/cli/main.go -bucket test-bucket -dir sample -checkpoint /tmp
```

This will mirror the contents of the directory `sample` into the root of bucket `test-bucket` and store the checkpoint files for the multipart-uploads into the `/tmp`.
