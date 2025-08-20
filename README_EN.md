# S3Copy Tool

## A Powerful S3 Copy Tool for Efficient Data Migration

S3Copy is a powerful command-line tool designed for efficient data copying to S3 storage. Whether from local files, HTTP URLs, or other S3 storages, it provides fast and reliable transfer experience with advanced features like resume capability and multipart upload.

## Core Features

- 🚀 **Multi-source Support**: Copy freely between local files/directories, HTTP/HTTPS URLs, and S3 buckets
- 🔍 **Smart Skipping**: Automatically skip existing files based on ETag comparison
- 📦 **Multipart Upload**: Automatically use multipart upload for files larger than 32MB
- 🔄 **Resume Capability**: Resume transfers from where they left off after interruption
- ⚡ **Concurrent Operations**: Configurable number of concurrent uploads to maximize bandwidth utilization
- 📊 **Real-time Progress**: Output progress in JSON format every second for easy monitoring
- 🧠 **Memory Optimization**: Stream large files in chunks to avoid memory overflow
- 🪣 **Automatic Bucket Creation**: Automatically create destination bucket if it doesn't exist
- 🔁 **Intelligent Retries**: Automatically retry on network failures with configurable retry count
- 🌐 **Same-Region Optimization**: Uses CopyObject API for same-account, same-region S3 copies to save bandwidth and improve performance

## Quick Start

### Installation Steps

```bash
# Install dependencies (if needed)
npm install

# Download Go modules
go mod download

# Build the project
go build -o s3copy .
```

### Environment Variables Configuration

Set the following environment variables for authentication:

```bash
# Source S3 configuration (required when copying from S3)
export SRC_ACCESS_KEY=your_source_access_key
export SRC_SECRET_KEY=your_source_secret_key
export SRC_S3_REGION=source_region  # Optional, default: us-east-1

# Destination S3 configuration (always required)
export DST_ACCESS_KEY=your_destination_access_key
export DST_SECRET_KEY=your_destination_secret_key
export DST_S3_REGION=destination_region  # Optional, default: us-east-1
```

## Usage Examples

### Copy from Local File/Directory to S3

```bash
# Copy a single file
s3copy -from-file /path/to/file.txt -to http://region.s3.com/oss1001

# Copy an entire directory
s3copy -from-file /path/to/directory -to http://region.s3.com/oss1001/directory
```

### Copy from URL to S3

```bash
# Copy from HTTP/HTTPS URL
s3copy -from-url https://example.com/file.zip -to http://region.s3.com/oss1001
```

### Copy from S3 to S3

```bash
# Copy across S3 buckets
s3copy -from-s3 http://oss1001.region.s4.comm -to http://region.s3.com/oss1001
```

## Configuration Options

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `-T`, `--concurrent` | Number of concurrent uploads | 10 |
| `--part-size` | Part size for multipart upload in bytes | 33554432 (32MB) |
| `-q`, `--quiet` | Quiet mode (no output) | false |
| `-v`, `--verbose` | Increase verbosity: -v for INFO, -vv for DEBUG, -vvv for TRACE | 0 |
| `--max-retries` | Number of retries for failed uploads | 3 |

### Advanced Usage Example

```bash
# Copy with custom settings
s3copy -from-file /large-dataset \
  -to http://region.s3.com/oss1001/backup \
  -T 20 \
  --part-size 67108864 \
  --max-retries 5 \
  -v
```

## Endpoint Format Support

The tool supports multiple endpoint formats:

1. **Bucket as subdomain**: `http://bucket.endpoint.com`
2. **Bucket in path**: `http://endpoint.com/bucket`
3. **Bucket with prefix**: `http://endpoint.com/bucket/prefix`

**Examples**:
- `http://oss1001.region.s3.com`
- `http://region.s3.com/oss1001`
- `http://region.s3.com/oss1001/backup/`

## Progress Output

The tool outputs progress in JSON format every second:

```json
{
  "total_size": 1048576000,  // Total bytes to transfer
  "migrated_size": 524288000, // Bytes already transferred
  "migrated_objects": 50,     // Number of objects completed
  "average_speed": 10485760,  // Average transfer speed in bytes/second
  "progress": 50.00           // Completion percentage
}
```

## Resume Capability

If a transfer is interrupted, simply run the same command again to resume:

1. Automatically detect transferred files and parts
2. Skip already completed files
3. Resume incomplete multipart uploads from where they left off

## ETag Checking

The tool automatically compares ETags to avoid unnecessary transfers:

- For local files: Calculates MD5 hash
- For S3 objects: Uses existing ETag
- For HTTP sources: Uses ETag from response headers (if available)

## Memory Optimization for Large HTTP Files

When handling large HTTP files, the tool employs these strategies:

1. **Chunked Streaming**: Downloads and uploads simultaneously in configurable part sizes
2. **Memory Control**: Uses fixed-size buffers to avoid memory accumulation
3. **Resume Support**: Can resume interrupted downloads from the last completed part
4. **Efficient Buffering**: Optimizes buffer usage to balance performance and memory footprint

## Automatic Bucket Creation

If the destination bucket does not exist, the tool will automatically create it before starting the upload, eliminating the need for manual creation.

## Retry Mechanism

The tool includes an intelligent retry mechanism that retries failed uploads up to 3 times by default, with a 3-second delay between attempts. You can customize the number of retries using the `--max-retries` parameter.

## Log Level Control

The tool supports controlling the verbosity of log output via the `-v` parameter. The meanings of different levels are as follows:

| Parameter | Log Level | Description |
|-----------|-----------|-------------|
| Without `-v` | ERROR/WARN | Only output errors or warnings |
| `-v` | INFO | Output main process information (e.g., "Start upload", "Finish download") |
| `-vv` | DEBUG | Output detailed debugging information (e.g., request headers, parameters) |
| `-vvv` | TRACE | Output most detailed information (e.g., full request/response bodies, stacks) |

## Error Handling

The tool handles various error conditions:

- Network interruptions (with configurable retry logic)
- Authentication errors
- Permission issues
- Memory constraints for large files
- Invalid endpoint formats

All errors are logged with descriptive messages, and internal state is updated to maintain consistency.

## Performance Optimization Tips

1. **Concurrent Uploads**: Increase `-T` for better throughput (balance with system resources)
2. **Part Size**: Larger parts reduce API calls but increase memory usage
3. **Network Selection**: Use endpoints in the same region for better performance
4. **Memory Usage**: The tool is designed to use minimal memory regardless of file size
5. **Retry Settings**: Adjust `--max-retries` based on network stability
6. **Same-Region Copies**: For S3-to-S3 copies within the same account and region, the tool automatically uses CopyObject API which is more efficient and saves bandwidth

## License

AGPL 3 License