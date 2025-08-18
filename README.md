# S3Copy Tool

A powerful S3 copy tool with advanced features including multipart upload, resume capability, ETag checking, and concurrent operations.

## Features

- **Multiple Source Types**: Support for local files/directories, HTTP/HTTPS URLs, and S3 buckets
- **ETag Checking**: Automatically skip files that already exist based on ETag comparison
- **Multipart Upload**: Automatically uses multipart upload for files larger than 32MB
- **Resume Capability**: Interrupt and resume transfers without losing progress
- **Concurrent Operations**: Configurable number of concurrent uploads for better performance
- **Progress Reporting**: Real-time progress reporting in JSON format
- **Memory Efficient**: Streams large HTTP files in chunks to avoid memory issues

## Installation

```bash
go mod download
go build -o s3copy .
```

## Environment Variables

Set the following environment variables for authentication:

```bash
# For source S3 (when copying from S3)
export SRC_ACCESS_KEY=your_source_access_key
export SRC_SECRET_KEY=your_source_secret_key

# For destination S3 (always required)
export DST_ACCESS_KEY=your_dest_access_key
export DST_SECRET_KEY=your_dest_secret_key
```

## Usage

### Copy from Local File/Directory

```bash
# Copy local file to S3
./s3copy -from-file /path/to/file.txt -to http://region.s3.com/oss1001

# Copy local directory to S3
./s3copy -from-file /path/to/directory -to http://region.s3.com/oss1001/directory
```

### Copy from URL

```bash
# Copy from HTTP/HTTPS URL to S3
./s3copy -from-url https://example.com/file.zip -to http://region.s3.com/oss1001
```

### Copy from S3 to S3

```bash
# Copy from S3 bucket to another S3 bucket
./s3copy -from-s3 http://oss1001.region.s4.comm -to http://region.s3.com/oss1001
```

### Configuration Options

```bash
--concurrent int         Number of concurrent uploads (default 10)
--part-size int          Part size for multipart upload in bytes (default 33554432)
--resume-file string     Resume file path (default ".s3copy-resume.json")
--skip-existing          Skip files that already exist (default true)
```

### Advanced Usage

```bash
# Copy with custom settings
./s3copy -from-file /large-dataset \
  -to http://region.s3.com/oss1001/backup \
  --concurrent 20 \
  --part-size 67108864 \
  --skip-existing=true
```

## Endpoint Format Support

The tool supports multiple endpoint formats:

1. **Bucket as subdomain**: `http://bucket.endpoint.com`
2. **Bucket in path**: `http://endpoint.com/bucket`
3. **Bucket with prefix**: `http://endpoint.com/bucket/prefix`

Examples:
- `http://oss1001.region.s3.com`
- `http://region.s3.com/oss1001`
- `http://region.s3.com/oss1001/backup/`

## Progress Output

The tool outputs progress in JSON format every second:

```json
{"total_size":1048576000,"migrated_size":524288000,"migrated_objects":50,"average_speed":10485760,"progress":50.00}
```

- `total_size`: Total bytes to transfer
- `migrated_size`: Bytes already transferred
- `migrated_objects`: Number of objects completed
- `average_speed`: Average transfer speed in bytes/second
- `progress`: Completion percentage

## Resume Capability

If a transfer is interrupted, you can resume it by running the same command again. The tool will:

1. Load the resume file (`.s3copy-resume.json` by default)
2. Skip already completed files
3. Resume incomplete multipart uploads from where they left off

## ETag Checking

The tool automatically compares ETags to avoid unnecessary transfers:

- For local files: Calculates MD5 hash
- For S3 objects: Uses existing ETag
- For HTTP sources: Uses ETag from response headers (if available)

## Memory Optimization for Large HTTP Files

When downloading large files from HTTP/HTTPS sources, the tool:

1. **Streams data in chunks**: Downloads and uploads simultaneously in configurable part sizes
2. **Avoids memory accumulation**: Never loads the entire file into memory
3. **Supports resume**: Can resume interrupted downloads from the last completed part
4. **Efficient buffering**: Uses fixed-size buffers to control memory usage

This approach prevents memory overflow (OOM) issues when handling very large files.

## Examples

### Environment Setup

```bash
export SRC_ACCESS_KEY=xxxx
export SRC_SECRET_KEY=xxx
export DST_ACCESS_KEY=xxxx
export DST_SECRET_KEY=xxxx
```

### Copy Examples

```bash
# Copy local directory to S3
./s3copy -from-file /local/path -to http://oss1001.region.s3.com

# Copy from URL to S3
./s3copy -from-url https://example.com/file.zip -to http://region.s3.com/oss1001

# Copy S3 to S3
./s3copy -from-s3 http://oss1001.region.s4.com -to http://region.s3.com/oss1001
```

## Error Handling

The tool handles various error conditions:

- Network interruptions (with retry logic)
- Authentication errors
- Permission issues
- Memory constraints for large files
- Invalid endpoint formats

All errors are logged with descriptive messages, and the resume file is updated to maintain consistency.

## Performance Tips

1. **Concurrent Uploads**: Increase `--concurrent` for better throughput (balance with system resources)
2. **Part Size**: Larger parts reduce API calls but use more memory
3. **Network**: Use endpoints in the same region for better performance
4. **Memory**: The tool is designed to use minimal memory regardless of file size

## License

MIT License