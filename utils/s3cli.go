package utils

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	source "s3copy/filesource"
)

// 生产者 队列
type PubData struct {
	PartNumber int64
	Data       []byte
	ReadError  error // 用于传递读取错误
}

// 结果队列
type RetData struct {
	PartNumber int64
	ETag       string
	Error      error
}

// ProgressCallback 是上传进度回调函数类型
type UlProgressCb func(partNumber int32, totalParts int32, uploadedBytes int64, totalBytes int64)

var (
	logger = GetLogger("s3copy")
)

type S3Cli struct {
	cfg        *source.EndpointConfig
	MaxRetries int
	Concurrent int   // 并发上传数量
	PartSize   int64 // 传输分块大小
	s3Client   *s3.S3
}

// 创建并返回一个新的 S3 客户端
func Create(config *source.EndpointConfig, maxRetries, partSize, concurrent int) (*S3Cli, error) {
	cli := &S3Cli{
		cfg:        config,
		MaxRetries: maxRetries,
		PartSize:   int64(partSize),
		Concurrent: concurrent,
	}
	// 创建 S3 客户端
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		DisableSSL:       aws.Bool(!strings.HasPrefix(config.Endpoint, "https")),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	cli.s3Client = s3.New(sess)
	return cli, nil
}

func (s *S3Cli) IsObjectExist(ctx context.Context, objectPath string, srcEtag string) (bool, string, error) {
	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	head, err := s.s3Client.HeadObjectWithContext(t, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(objectPath),
	})
	if err != nil {
		log.Printf("failed to check if object exists: %v", err)
		return false, "", fmt.Errorf("failed to check if object exists: %w", err)
	}

	dstEtag := ""
	// 如果对象存在且 ETag 匹配，直接返回成功
	if head != nil && head.ETag != nil {
		dstEtag = *head.ETag
	}
	if strings.Trim(srcEtag, `"`) == strings.Trim(dstEtag, `"`) {
		log.Printf("Object %s already exists with matching ETag (%s), skipping upload",
			objectPath, srcEtag)
		return true, dstEtag, nil
	}
	log.Printf("etag %s not matching ETag (%s)", srcEtag, head)
	return false, dstEtag, errors.New("etag not match")
}

// uploadSimple 执行简单上传
func (s *S3Cli) UploadObject(ctx context.Context, objectPath string, data io.ReadCloser, srcmeta map[string]string) error {
	var srcEtag, dstEtag string
	if srcmeta != nil {
		srcEtag = srcmeta["etag"]
	}

	if srcEtag != "" {
		exist, e, err := s.IsObjectExist(ctx, objectPath, srcEtag)
		dstEtag = e
		if err == nil && exist {
			return nil
		}
	}

	// 将流式数据读入内存
	buf, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read data from source: %w", err)
	}

	if srcEtag == "" {
		// 计算数据的 MD5 哈希作为 ETag
		hash := md5.Sum(buf)
		srcEtag = fmt.Sprintf("\"%x\"", hash) // S3 ETag 格式是带引号的十六进制字符串
	}
	if len(srcEtag) > 0 && len(dstEtag) > 0 && strings.Trim(srcEtag, `"`) == strings.Trim(dstEtag, `"`) {
		return nil
	}
	exist, dstEtag, err := s.IsObjectExist(ctx, objectPath, srcEtag)
	if err == nil && exist {
		return nil
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(objectPath),
		Body:   bytes.NewReader(buf), // ✅ *bytes.Reader 实现了 io.ReadSeeker
	}

	// 添加元数据
	if len(srcmeta) > 0 {
		metadataMap := make(map[string]*string)
		for key, value := range srcmeta {
			metadataMap[key] = aws.String(value)
		}
		params.Metadata = metadataMap
	}
	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	// 执行上传
	_, err = s.s3Client.PutObjectWithContext(t, params)
	if err != nil {
		return fmt.Errorf("failed to upload object %s: %w", objectPath, err)
	}

	return nil
}

// UploadMultipart 上传文件到 S3，使用分片并行上传方式，并支持快速失败。
func (s *S3Cli) UploadMultipart(ctx context.Context, objectPath string, data io.ReadCloser, srcmeta map[string]string) error {
	// 1. 解析源文件大小
	srcSizeStr, ok := srcmeta["size"]
	if !ok {
		return fmt.Errorf("missing source size in metadata")
	}
	srcSize, err := strconv.ParseInt(srcSizeStr, 10, 64)
	if err != nil || srcSize <= 0 {
		return fmt.Errorf("invalid source size in metadata: %v", err)
	}

	// 2. 动态设置超时（基于文件大小），至少60秒
	calculatedTimeoutSeconds := int(math.Max(float64(srcSize/(1024*1024)), 1)) * 60
	timeout := time.Duration(calculatedTimeoutSeconds) * time.Second
	if timeout < 60*time.Second {
		timeout = 60 * time.Second
	}
	uploadCtx, cancelUploadCtx := context.WithTimeout(ctx, timeout)
	defer cancelUploadCtx()

	logger.Infof("Starting multipart upload for %s with timeout %v", objectPath, timeout)

	// 3. 检查目标对象是否存在且大小匹配 (简化)
	// ... (检查逻辑保持不变)

	// 4. 初始化分片上传
	uploadID, initErr := s.InitPart(uploadCtx, s.cfg.Bucket, objectPath)
	if initErr != nil {
		return fmt.Errorf("failed to init multipart upload: %w", initErr)
	}
	logger.Infof("Initialized multipart upload for %s with ID: %s", objectPath, uploadID)

	// 5. 确保在函数退出时（无论成功或失败）中止上传（如果 uploadID 仍然存在）
	defer func() {
		if uploadID != "" {
			abortCtx, cancelAbortCtx := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelAbortCtx()
			abortErr := s.AbortMultipartUpload(abortCtx, s.cfg.Bucket, objectPath, uploadID)
			if abortErr != nil {
				logger.Errorf("Failed to abort multipart upload (ID: %s) for %s: %v", uploadID, objectPath, abortErr)
			} else {
				logger.Infof("Aborted multipart upload (ID: %s) for %s", uploadID, objectPath)
			}
		}
	}()

	// 6. 准备上传参数
	partSize := s.PartSize
	if partSize <= 0 {
		partSize = 5 * 1024 * 1024 // 默认 5MB
	}
	maxConcurrency := s.Concurrent
	if maxConcurrency <= 0 {
		maxConcurrency = 5 // 默认并发数
	}
	logger.Infof("Preparing to upload %s (%d bytes), part size %d, concurrency %d", objectPath, srcSize, partSize, maxConcurrency)

	partETags := make([]*s3.CompletedPart, 0)

	// --- 设置 Channel (生产者-消费者模型) ---
	pubChan := make(chan PubData, maxConcurrency*2) // 缓冲 channel
	retChan := make(chan RetData, maxConcurrency*2) // 结果 channel

	// --- 7. 创建用于快速失败的上下文 ---
	// workCtx 用于控制所有 worker 的生命周期
	// 一旦发生错误，调用 workCancel 会取消 workCtx，进而取消所有派生的上下文
	workCtx, workCancel := context.WithCancel(uploadCtx)
	defer workCancel() // 确保函数退出时取消，防止泄露

	// --- 8. 启动生产者 Goroutine (读取数据) ---
	go func() {
		defer close(pubChan) // 读取完成后关闭 channel
		partNum := int64(1)
		for {
			// 检查父上下文或工作上下文是否已取消
			select {
			case <-uploadCtx.Done():
				logger.Errorf("Upload context cancelled during reading: %v", uploadCtx.Err())
				workCancel() // 确保取消被触发
				return
			case <-workCtx.Done():
				logger.Errorf("Work context cancelled during reading: %v", workCtx.Err())
				return // 如果已被取消，则直接退出
			default:
			}

			buffer := make([]byte, partSize)
			n, readErr := io.ReadFull(data, buffer)

			if n > 0 {
				pubChan <- PubData{
					PartNumber: partNum,
					Data:       buffer[:n],
					ReadError:  nil,
				}
				partNum++
			}

			if readErr != nil {
				if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
					logger.Infof("Finished reading data.")
					break
				} else {
					logger.Errorf("Error reading data: %v", readErr)
					// --- 关键修改：遇到读取错误立即取消所有工作 ---
					workCancel() // 触发快速失败
					return       // 生产者 goroutine 退出
				}
			}
		}
	}()

	// --- 9. 启动消费者 Worker Pool (并发上传) ---
	var wg sync.WaitGroup
	limiter := make(chan struct{}, maxConcurrency) // 信号量

	// 启动固定数量的 worker
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logger.Infof("Started upload worker %d", workerID)
			for part := range pubChan { // 从 channel 读取任务
				// 检查上下文是否已取消，即使在等待 channel 时
				select {
				case <-workCtx.Done():
					logger.Infof("Worker %d: Work context cancelled, exiting.", workerID)
					return // 如果上下文已取消，worker 退出
				default:
				}

				if part.ReadError != nil {
					// 理论上不会到达这里，因为生产者在读取错误时已经取消了上下文
					// 但为了健壮性，我们仍然处理它
					retChan <- RetData{PartNumber: 0, ETag: "", Error: part.ReadError}
					workCancel() // 再次触发取消（冗余但安全）
					return
				}

				// 为每个 UploadPart 调用创建一个带超时的上下文
				partCtx, partCancel := context.WithTimeout(workCtx, 30*time.Second)

				limiter <- struct{}{} // 获取令牌
				// 执行上传重试逻辑
				var etag string
				var uploadPartErr error
				for attempt := 0; attempt < s.MaxRetries; attempt++ {
					// 检查 partCtx 是否已取消（例如，由于快速失败）
					select {
					case <-partCtx.Done():
						uploadPartErr = partCtx.Err()
						logger.Errorf("Worker %d: Part context cancelled before attempt %d for part %d: %v", workerID, attempt+1, part.PartNumber, uploadPartErr)
						break // 跳出重试循环
					default:
					}

					// 使用 partCtx，它会响应 workCtx 的取消
					etag, uploadPartErr = s.UploadPart(partCtx, s.cfg.Bucket, objectPath, uploadID, part.PartNumber, part.Data)
					if uploadPartErr == nil {
						logger.Infof("Worker %d: successfully uploaded part %d (%d bytes)", workerID, part.PartNumber, len(part.Data))
						break
					}
					logger.Errorf("Worker %d: Failed to upload part %d (attempt %d/%d): %v", workerID, part.PartNumber, attempt+1, s.MaxRetries, uploadPartErr)

					// 在重试前检查上下文是否已取消
					select {
					case <-partCtx.Done():
						uploadPartErr = partCtx.Err()
						logger.Errorf("Worker %d: Context cancelled during retries for part %d: %v", workerID, part.PartNumber, uploadPartErr)
						break // 跳出重试循环
					case <-time.After(2 * time.Second):
						// 等待后重试
					}
				}
				partCancel() // 释放 partCtx
				<-limiter    // 释放令牌

				// 将结果发送回主 goroutine
				retChan <- RetData{
					PartNumber: part.PartNumber,
					ETag:       etag,
					Error:      uploadPartErr,
				}

				// --- 关键：如果上传失败，立即取消所有工作 ---
				if uploadPartErr != nil {
					logger.Errorf("Worker %d: Failing fast due to error in part %d", workerID, part.PartNumber)
					workCancel() // 触发取消
					// 注意：不要在这里 return，让 for range 循环自然结束
					// 因为 channel 可能还有其他任务，我们需要让它们也感知到取消
				}
			}
			logger.Infof("Upload worker %d finished", workerID)
		}(i)
	}

	// --- 10. 启动 Goroutine 等待所有 Worker 完成并关闭结果 Channel ---
	go func() {
		wg.Wait()
		close(retChan) // 所有 worker 完成后关闭结果 channel
	}()

	// --- 11. 收集并处理上传结果 (在主 Goroutine 中) ---
	var finalError error
	for ret := range retChan {
		if ret.Error != nil {
			logger.Errorf("Upload failed for part %d: %v", ret.PartNumber, ret.Error)
			if finalError == nil {
				finalError = fmt.Errorf("upload failed for part %d: %w", ret.PartNumber, ret.Error)
				// 一旦发现第一个错误，立即调用取消，加速失败过程
				// (虽然生产者/worker 内部已经调用了，但这里再调用一次更保险)
				workCancel()
			}
			// 继续读取 retChan 直到它被关闭，以防止 goroutine 泄露
		} else {
			partETags = append(partETags, &s3.CompletedPart{
				ETag:       aws.String(ret.ETag),
				PartNumber: aws.Int64(ret.PartNumber),
			})
		}
	}

	// --- 12. 检查最终状态 ---
	if finalError != nil {
		return finalError // 返回第一个遇到的错误
	}

	if len(partETags) == 0 {
		return fmt.Errorf("no data parts were read or uploaded for %s", objectPath)
	}

	// --- 13. 收集并排序分片结果（按 PartNumber） ---
	sort.Slice(partETags, func(i, j int) bool {
		return *partETags[i].PartNumber < *partETags[j].PartNumber
	})

	// --- 14. 完成分片上传 ---
	completeErr := s.CompletePart(uploadCtx, s.cfg.Bucket, objectPath, uploadID, partETags)
	if completeErr != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", completeErr)
	}

	// --- 15. 成功完成 ---
	logger.Infof("Successfully completed multipart upload for %s with %d parts", objectPath, len(partETags))
	uploadID = "" // 防止 defer 中执行 Abort
	return nil
}

func (s *S3Cli) HeadMultipartUpload(ctx context.Context, objectPath string) (*s3.MultipartUpload, error) {
	t, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	params := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.cfg.Bucket),
		// 可选：用 Prefix 提高效率（如果 key 有共同前缀）
		Prefix: aws.String(objectPath),
	}

	var upload *s3.MultipartUpload
	err := s.s3Client.ListMultipartUploadsPagesWithContext(t, params,
		func(page *s3.ListMultipartUploadsOutput, lastPage bool) bool {
			for _, u := range page.Uploads {
				// 精确匹配 Key
				if *u.Key == objectPath {
					upload = u
					return false // 停止分页
				}
			}
			return true // 继续下一页
		})

	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	if upload == nil {
		return nil, nil // 未找到未完成的上传
	}

	return upload, nil
}

// ListParts 获取当前上传的所有已上传分片
func (s *S3Cli) ListParts(ctx context.Context, bucketName, objectKey, uploadID string) ([]*s3.Part, error) {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return nil, errors.New("input params is empty")
	}

	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var allParts []*s3.Part
	var partNumberMarker *int64 = nil
	const maxParts = 10000
	for {
		resp, err := s.s3Client.ListPartsWithContext(t, &s3.ListPartsInput{
			Bucket:           aws.String(bucketName),
			Key:              aws.String(objectKey),
			UploadId:         aws.String(uploadID),
			MaxParts:         aws.Int64(int64(maxParts)),
			PartNumberMarker: partNumberMarker,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list parts: %w", err)
		}

		allParts = append(allParts, resp.Parts...)

		if resp.IsTruncated != nil && *resp.IsTruncated && resp.NextPartNumberMarker != nil {
			partNumberMarker = resp.NextPartNumberMarker
		} else {
			break
		}
	}

	logger.Infof("ListParts completed, total parts: %d", len(allParts))
	return allParts, nil
}

// InitPart 初始化分片上传任务
func (s *S3Cli) InitPart(ctx context.Context, bucketName, objectKey string) (string, error) {
	if objectKey == "" || bucketName == "" {
		return "", errors.New("empty object key or bucket name")
	}

	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger.Infof("Initializing multipart upload for %s/%s", bucketName, objectKey)

	resp, err := s.s3Client.CreateMultipartUploadWithContext(t, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return "", fmt.Errorf("failed to init multipart upload: %w", err)
	}

	logger.Infof("InitPart success: UploadId=%s", *resp.UploadId)
	return *resp.UploadId, nil
}

// UploadPart 上传单个分片并返回 ETag
func (s *S3Cli) UploadPart(ctx context.Context, bucketName, objectKey, uploadID string, partNumber int64, data []byte) (string, error) {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return "", errors.New("input params is empty")
	}
	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger.Infof("start uploading %d bytes to part %d of %s/%s", len(data), partNumber, bucketName, objectKey)

	resp, err := s.s3Client.UploadPartWithContext(t, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(partNumber),
		Body:       bytes.NewReader(data),
		UploadId:   aws.String(uploadID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload part %d: %w", partNumber, err)
	}

	logger.Infof("successfully uploaded %s/%s part %d", bucketName, objectKey, partNumber)
	return *resp.ETag, nil
}

// CompletePart 完成分片上传
func (s *S3Cli) CompletePart(ctx context.Context, bucketName, objectKey, uploadID string, uploadedParts []*s3.CompletedPart) error {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return errors.New("input params is empty")
	}

	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 排序 Parts
	sortedParts := make([]*s3.CompletedPart, len(uploadedParts))
	copy(sortedParts, uploadedParts)
	for i := range sortedParts {
		for j := i + 1; j < len(sortedParts); j++ {
			if *sortedParts[i].PartNumber > *sortedParts[j].PartNumber {
				sortedParts[i], sortedParts[j] = sortedParts[j], sortedParts[i]
			}
		}
	}

	multipartUpload := s3.CompletedMultipartUpload{
		Parts: sortedParts,
	}

	logger.Infof("Completing multipart upload for %s/%s", bucketName, objectKey)

	_, err := s.s3Client.CompleteMultipartUploadWithContext(t, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(objectKey),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &multipartUpload,
	})

	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	logger.Infof("Completed multipart upload for %s/%s", bucketName, objectKey)
	return nil
}

// AbortMultipartUpload 中止指定的分片上传任务
func (s *S3Cli) AbortMultipartUpload(ctx context.Context, bucketName, objectKey, uploadID string) error {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return errors.New("input params is empty")
	}
	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger.Infof("Aborting multipart upload: UploadId=%s", uploadID)

	_, err := s.s3Client.AbortMultipartUploadWithContext(t, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	logger.Infof("Successfully aborted multipart upload: UploadId=%s", uploadID)
	return nil
}

// IsUploadIDExist 检查指定的上传ID是否存在
func (s *S3Cli) IsUploadIDExist(ctx context.Context, bucket, key, uploadID string) (bool, error) {
	if bucket == "" || key == "" || uploadID == "" {
		return false, errors.New("input params is empty")
	}

	// 创建带超时的子 context（可选）
	t, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := s.s3Client.ListPartsWithContext(t, &s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MaxParts: aws.Int64(1),
	})

	if err != nil {
		var noSuchUpload *types.NoSuchUpload
		if errors.As(err, &noSuchUpload) {
			return false, nil
		}
		return false, fmt.Errorf("failed to list parts: %w", err)
	}

	return true, nil
}
