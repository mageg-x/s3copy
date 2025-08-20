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
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"s3copy/copier"
	"s3copy/utils"

	"github.com/spf13/cobra"
)

var (
	fromFile   string
	fromURL    string
	fromS3     string
	to         string
	concurrent int
	partSize   int64
	quiet      bool
	verbose    int
	logger     = utils.GetLogger("s3copy")
	progress   = utils.GetProgress()
)

func main() {
	utils.SetOutput(os.Stdout)
	var rootCmd = &cobra.Command{
		Use:   "s3copy",
		Short: "A powerful S3 copy tool with multipart upload and resume capabilities",
		Long: `S3Copy is a tool for copying files from various sources to S3-compatible storage.
Supports local files/directories, URLs, and S3 buckets as sources.
Features include ETag checking, multipart upload, resume capability, and concurrent operations.

Environment Variables:
  SRC_ACCESS_KEY - Source access key
  SRC_SECRET_KEY - Source secret key  
  SRC_S3_REGION  - Source region
  DST_ACCESS_KEY - Destination access key
  DST_SECRET_KEY - Destination secret key
  DST_S3_REGION  - Destination region`,
		Run: runS3Copy,
	}

	rootCmd.Flags().StringVar(&fromFile, "from-file", "", "Copy from local file or directory")
	rootCmd.Flags().StringVar(&fromURL, "from-url", "", "Copy from HTTP/HTTPS URL")
	rootCmd.Flags().StringVar(&fromS3, "from-s3", "", "Copy from S3 bucket (http://bucket.endpoint or http://endpoint/bucket)")
	rootCmd.Flags().StringVar(&to, "to", "", "Destination S3 endpoint (required)")
	rootCmd.Flags().IntVar(&concurrent, "T", 10, "Number of concurrent uploads")
	rootCmd.Flags().Int64Var(&partSize, "part-size", 32*1024*1024, "Part size for multipart upload (bytes)")
	rootCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "quiet mode [Long and short option]")
	rootCmd.Flags().CountVarP(&verbose, "verbose", "v", "Increase verbosity: -v for INFO, -vv for DEBUG, -vvv for TRACE")
	rootCmd.MarkFlagRequired("to")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(100)
	}
}

func runS3Copy(*cobra.Command, []string) {
	// Validate that exactly one source is specified
	sourceCount := 0
	var source, sourceType string

	// Set log level based on verbose flag
	if quiet {
		utils.SetOutFile("/dev/null")
	} else {
		switch verbose {
		case 0:
			// Default level: ERROR/WARN
			utils.SetLogLevel(logrus.WarnLevel)
		case 1:
			// INFO level
			utils.SetLogLevel(logrus.InfoLevel)
		case 2:
			// DEBUG level
			utils.SetLogLevel(logrus.DebugLevel)
		case 3:
			// TRACE level
			utils.SetLogLevel(logrus.TraceLevel)
		default:
			// More than 3 v's, use TRACE
			utils.SetLogLevel(logrus.TraceLevel)
		}
	}

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
		os.Exit(100)
	}
	if sourceCount > 1 {
		logger.Errorf("error: can only specify one source type")
		os.Exit(100)
	}

	if to == "" {
		logger.Errorf("error: --to is required")
		os.Exit(100)
	}
	if sourceType == "http" && !utils.IsNormalFile(fromURL) {
		fmt.Printf("SRC ACCESS KEY can not be empty!")
		os.Exit(105)
	}
	// 检查环境变量是否存在
	if sourceType == "s3" && os.Getenv("SRC_ACCESS_KEY") == "" {
		fmt.Printf("SRC ACCESS KEY can not be empty!")
		os.Exit(101)
	}
	if sourceType == "s3" && os.Getenv("SRC_SECRET_KEY") == "" {
		fmt.Printf("SRC SECRET KEY can not be empty!")
		os.Exit(102)
	}
	if os.Getenv("DST_ACCESS_KEY") == "" {
		fmt.Printf("DST ACCESS KEY can not be empty!")
		os.Exit(103)
	}
	if os.Getenv("DST_SECRET_KEY") == "" {
		fmt.Printf("DST SECRET KEY can not be empty!")
		os.Exit(104)
	}

	copyOpt := copier.CopyOptions{
		SourcePath: source,
		SourceType: sourceType,
		DestPath:   to,
		DestType:   "s3",
		Concurrent: concurrent,
		PartSize:   partSize,
	}

	if sourceType == "file" || sourceType == "http" {
		// 转换为绝对路径
		absPath, err := filepath.Abs(source)
		if err != nil {
			logger.Fatalf("failed to get absolute path for %s: %v", source, err)
			os.Exit(106)
		}
		realPath, err := filepath.EvalSymlinks(absPath)
		if err != nil {
			logger.Fatalf("failed to get absolute path for %s: %v", source, err)
			os.Exit(106)
		}
		copyOpt.SourcePath = realPath
	}

	cp, err := copier.NewCopier(&copyOpt)
	if err != nil || cp == nil {
		logger.Fatalf("new copier failed: %v", err)
		os.Exit(107)
	}

	err = cp.Copy()
	if err != nil {
		logger.Fatalf("copy failed: %v", err)
		os.Exit(1)
	}
	//结束前打印一次输出进度
	progress.Report("")
}
