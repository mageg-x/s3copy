package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"s3copy/copier"
)

var (
	fromFile     string
	fromURL      string
	fromS3       string
	to           string
	concurrent   int
	partSize     int64
	resumeFile   string
	skipExisting bool
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
	rootCmd.Flags().IntVar(&concurrent, "concurrent", 10, "Number of concurrent uploads")
	rootCmd.Flags().Int64Var(&partSize, "part-size", 32*1024*1024, "Part size for multipart upload (bytes)")
	rootCmd.Flags().StringVar(&resumeFile, "resume-file", ".s3copy-resume.json", "Resume file path")
	rootCmd.Flags().BoolVar(&skipExisting, "skip-existing", true, "Skip files that already exist (ETag check)")

	rootCmd.MarkFlagRequired("to")

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
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
		fmt.Println("Error: Must specify one of --from-file, --from-url, or --from-s3")
		os.Exit(1)
	}
	if sourceCount > 1 {
		fmt.Println("Error: Can only specify one source type")
		os.Exit(1)
	}

	if to == "" {
		fmt.Println("Error: --to is required")
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
		log.Fatal("new copier failed: ", err)
	}

	err = copier.Copy()
	if err != nil {
		log.Fatal("copy failed: ", err)
	}

}
