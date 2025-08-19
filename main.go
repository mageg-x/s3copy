// Copyright (C) 2025 raochaoxun <raochaoxun@gmail.com>
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"github.com/spf13/cobra"
	"os"
	"s3copy/copier"
	"s3copy/utils"
)

var (
	fromFile   string
	fromURL    string
	fromS3     string
	to         string
	concurrent int
	partSize   int64

	logger = utils.GetLogger("s3copy")
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "s3copy",
		Short: "A powerful S3 copy tool with multipart upload and resume capabilities",
		Long: `S3Copy is a tool for copying files from various sources to S3-compatible storage.
Supports local files/directories, URLs, and S3 buckets as sources.
Features include ETag checking, multipart upload, resume capability, and concurrent operations.

Environment Variables:
  SRC_ACCESS_KEY - Source access key
  SRC_SECRET_KEY - Source secret key  
  DST_ACCESS_KEY - Destination access key
  DST_SECRET_KEY - Destination secret key`,
		Run: runS3Copy,
	}

	rootCmd.Flags().StringVar(&fromFile, "from-file", "", "Copy from local file or directory")
	rootCmd.Flags().StringVar(&fromURL, "from-url", "", "Copy from HTTP/HTTPS URL")
	rootCmd.Flags().StringVar(&fromS3, "from-s3", "", "Copy from S3 bucket (http://bucket.endpoint or http://endpoint/bucket)")
	rootCmd.Flags().StringVar(&to, "to", "", "Destination S3 endpoint (required)")
	rootCmd.Flags().IntVar(&concurrent, "T", 10, "Number of concurrent uploads")
	rootCmd.Flags().Int64Var(&partSize, "part-size", 32*1024*1024, "Part size for multipart upload (bytes)")
	rootCmd.MarkFlagRequired("to")

	if err := rootCmd.Execute(); err != nil {
		logger.Fatal(err)
	}
}

func runS3Copy(cmd *cobra.Command, args []string) {
	// Validate that exactly one source is specified
	sourceCount := 0
	var source, sourceType string

	if fromFile != "" {
		sourceCount++
		source = fromFile
		sourceType = "file"
	}
	if fromURL != "" {
		sourceCount++
		source = fromURL
		sourceType = "http"
	}
	if fromS3 != "" {
		sourceCount++
		source = fromS3
		sourceType = "s3"
	}

	if sourceCount == 0 {
		logger.Errorf("error: must specify one of --from-file, --from-url, or --from-s3")
		os.Exit(1)
	}
	if sourceCount > 1 {
		logger.Errorf("error: can only specify one source type")
		os.Exit(1)
	}

	if to == "" {
		logger.Errorf("error: --to is required")
		os.Exit(1)
	}

	copyOpt := copier.CopyOptions{
		SourcePath: source,
		SourceType: sourceType,
		DestPath:   to,
		DestType:   "s3",
		Concurrent: concurrent,
		PartSize:   partSize,
	}

	copier, err := copier.NewCopier(&copyOpt)
	if err != nil {
		logger.Fatalf("new copier failed: %v", err)
	}

	err = copier.Copy()
	if err != nil {
		logger.Fatalf("copy failed: %v", err)
	}

}
