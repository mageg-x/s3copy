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

package copier

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math"
	"s3copy/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconf "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	source "s3copy/filesource"
)

// PubData 生产者 队列
type PubData struct {
	PartNumber int64
	Data       []byte
	ReadError  error // 用于传递读取错误
}

// RetData 结果队列
type RetData struct {
	PartNumber int64
	ETag       string
	Error      error
}

type S3Cli struct {
	cfg        *source.EndpointConfig
	MaxRetries int
	Concurrent int   // 并发上传数量
	PartSize   int64 // 传输分块大小
	s3Client   *s3.Client
}

// Create 创建并返回一个新的 S3 客户端
func Create(config *source.EndpointConfig, maxRetries, partSize, concurrent int) (*S3Cli, error) {
	logger.Infof("Creating S3 client with config: bucket=%s, region=%s, endpoint=%s", config.Bucket, config.Region, config.Endpoint)
	startTime := time.Now()

	cli := &S3Cli{
		cfg:        config,
		MaxRetries: maxRetries,
		PartSize:   int64(partSize),
		Concurrent: concurrent,
	}

	ctx := context.Background()

	if config.AccessKey == "" || config.SecretKey == "" {
		logger.Errorf("Missing AWS credentials: access key or secret key is empty")
		return nil, fmt.Errorf("missing AWS credentials")
	}

	logger.Debugf("S3 client configuration: max retries=%d, part size=%d, concurrent=%d", maxRetries, partSize, concurrent)

	// 创建凭证
	logger.Debugf("Creating AWS credentials provider")
	credentialProvider := credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")

	// 加载配置，并指定凭证提供者
	logger.Debugf("Loading AWS SDK configuration with region: %s", config.Region)
	cfg, err := awsconf.LoadDefaultConfig(ctx,
		awsconf.WithRegion(config.Region),
		awsconf.WithCredentialsProvider(credentialProvider),
	)
	if err != nil {
		logger.Errorf("Failed to load SDK configuration: %v", err)
		return nil, fmt.Errorf("failed to load SDK configuration: %w", err)
	}

	// 创建 S3 客户端（对接 MinIO 等私有服务需加选项）
	logger.Debugf("Creating S3 client instance")
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// 如果配置了自定义端点，设置自定义端点
		if config.Endpoint != "" {
			logger.Debugf("Setting custom endpoint: %s", config.Endpoint)
			o.BaseEndpoint = aws.String(config.Endpoint)
		}
		// 对接 MinIO/OSS/COS 等需要PathStyle的情况
		logger.Debugf("Enabling path-style access")
		o.UsePathStyle = config.UsePathStyle
	})

	cli.s3Client = client
	logger.Infof("S3 client created successfully in %v", time.Since(startTime))
	return cli, nil
}

func (s *S3Cli) IsObjectExist(ctx context.Context, objectPath string, srcEtag string) (bool, string, error) {
	logger.Infof("Checking if object exists: %s/%s", s.cfg.Bucket, objectPath)
	startTime := time.Now()

	head, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(objectPath),
	})
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) {
			logger.Infof("Object does not exist: %s/%s", s.cfg.Bucket, objectPath)
			return false, "", nil
		}
		logger.Errorf("Failed to head object %s/%s: %v", s.cfg.Bucket, objectPath, err)
		return false, "", fmt.Errorf("failed to head object: %w", err)
	}

	logger.Debugf("Object exists: %s/%s", s.cfg.Bucket, objectPath)
	srcClean := strings.Trim(srcEtag, `"`)

	// 标准化元数据：统一转小写
	normalizedMeta := utils.NormalizeMetadata(head.Metadata)

	// 1. 检查标准 ETag
	if head.ETag != nil {
		dstClean := strings.Trim(*head.ETag, `"`)
		logger.Debugf("Checking ETag: source=%s, destination=%s", srcClean, dstClean)
		if dstClean == srcClean {
			logger.Infof("ETag match for object %s/%s", s.cfg.Bucket, objectPath)
			return true, dstClean, nil
		}
	}

	// 2. 检查标准化后的元数据
	if metaEtagPtr, ok := normalizedMeta["etag"]; ok && metaEtagPtr != nil {
		dstClean := strings.Trim(*metaEtagPtr, `"`)
		logger.Debugf("Checking metadata ETag: source=%s, destination=%s", srcClean, dstClean)
		if dstClean == srcClean {
			logger.Infof("Metadata ETag match for object %s/%s", s.cfg.Bucket, objectPath)
			return true, dstClean, nil
		}
	}

	finalEtag := ""
	if head.ETag != nil {
		finalEtag = *head.ETag
	}

	logger.Warningf("ETag not match for object %s/%s: source=%s, destination=%s", s.cfg.Bucket, objectPath, srcClean, finalEtag)
	logger.Infof("Object existence check completed in %v", time.Since(startTime))
	return false, finalEtag, errors.New("etag not match")
}

// UploadObject uploadSimple 执行简单上传
func (s *S3Cli) UploadObject(ctx context.Context, fs source.Source, from, to string, srcmeta map[string]string) (bool, error) {
	logger.Infof("Starting simple upload: from=%s to=%s/%s", from, s.cfg.Bucket, to)
	startTime := time.Now()

	var srcEtag, dstEtag string
	if srcmeta != nil {
		srcEtag = srcmeta["etag"]
		logger.Debugf("Source ETag from metadata: %s", srcEtag)
	}

	if srcEtag != "" {
		logger.Debugf("Checking if object exists with ETag: %s/%s", s.cfg.Bucket, to)
		exist, e, err := s.IsObjectExist(ctx, to, srcEtag)
		dstEtag = e
		if err == nil && exist {
			logger.Infof("Object %s/%s already exists with matching etag, skipping upload", s.cfg.Bucket, to)
			logger.Infof("Upload completed in %v", time.Since(startTime))
			return true, nil
		}
	}

	logger.Debugf("Opening reader for source: %s", from)
	reader, _, err := fs.Read(ctx, from, 0)
	if err != nil {
		logger.Errorf("Failed to read %s: %v", from, err)
		return false, fmt.Errorf("failed to open source %s", from)
	}
	defer reader.Close()

	// 将流式数据读入内存
	logger.Debugf("Reading data from source: %s", from)
	buf, err := io.ReadAll(reader)
	if err != nil {
		logger.Errorf("Failed to read object %s: %v", to, err)
		return false, fmt.Errorf("failed to read data from source: %w", err)
	}

	logger.Debugf("Read %d bytes from source", len(buf))

	if srcEtag == "" {
		// 计算数据的 MD5 哈希作为 ETag
		logger.Debugf("Calculating MD5 hash for source data")
		hash := md5.Sum(buf)
		srcEtag = fmt.Sprintf("\"%x\"", hash) // S3 ETag 格式是带引号的十六进制字符串
		logger.Debugf("Calculated ETag: %s", srcEtag)
	}
	if len(srcEtag) > 0 && len(dstEtag) > 0 && strings.Trim(srcEtag, `"`) == strings.Trim(dstEtag, `"`) {
		logger.Infof("Object %s/%s already exists with matching etag, skipping upload", s.cfg.Bucket, to)
		logger.Infof("Upload completed in %v", time.Since(startTime))
		return true, nil
	}

	logger.Debugf("Final check if object exists: %s/%s", s.cfg.Bucket, to)
	exist, dstEtag, err := s.IsObjectExist(ctx, to, srcEtag)
	if err == nil && exist {
		logger.Infof("Object %s/%s already exists with matching etag, skipping upload", s.cfg.Bucket, to)
		logger.Infof("Upload completed in %v", time.Since(startTime))
		return true, nil
	}

	logger.Debugf("Preparing to upload object: %s/%s", s.cfg.Bucket, to)
	params := &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(to),
		Body:   bytes.NewReader(buf), // ✅ *bytes.Reader 实现了 io.ReadSeeker
	}

	// 添加元数据
	if len(srcmeta) > 0 {
		logger.Debugf("Adding metadata to object: %d entries", len(srcmeta))
		metadataMap := make(map[string]string)
		for key, value := range srcmeta {
			metadataMap[key] = value
			logger.Debugf("Metadata: %s=%s", key, value)
		}
		params.Metadata = metadataMap
	}

	logger.Infof("Initiating upload to %s/%s with retries: %d", s.cfg.Bucket, to, s.MaxRetries)
	return false, utils.WithRetry(fmt.Sprintf("upload to dest object %s", to), s.MaxRetries, func() error {
		// 执行上传
		_, err := s.s3Client.PutObject(ctx, params)
		if err != nil {
			logger.Errorf("Failed to put object %s/%s: %v", s.cfg.Bucket, to, err)
			return fmt.Errorf("failed to put object: %w", err)
		}
		logger.Infof("Successfully uploaded object %s/%s in %v", s.cfg.Bucket, to, time.Since(startTime))
		return nil
	})
}

// UploadMultipart 上传文件到 S3，使用分片并行上传方式，并支持快速失败。
func (s *S3Cli) UploadMultipart(ctx context.Context, fs source.Source, from, to string, srcmeta map[string]string) (bool, error) {
	logger.Infof("Starting multipart upload: %s -> %s", from, to)
	startTime := time.Now()

	// 打开源文件读取器
	reader, _, err := fs.Read(ctx, from, 0)
	if err != nil {
		logger.Errorf("Failed to read source file %s: %v", from, err)
		return false, fmt.Errorf("failed to open source %s", from)
	}
	defer reader.Close()
	logger.Debugf("Successfully opened source file: %s", from)

	// 1. 解析源文件大小
	srcSizeStr, ok := srcmeta["size"]
	if !ok {
		logger.Errorf("Missing source size in metadata")
		return false, fmt.Errorf("missing source size in metadata")
	}
	srcSize, err := strconv.ParseInt(srcSizeStr, 10, 64)
	if err != nil || srcSize <= 0 {
		logger.Errorf("Invalid source size in metadata: %v", err)
		return false, fmt.Errorf("invalid source size in metadata: %v", err)
	}
	logger.Infof("Source file size determined: %d bytes", srcSize)

	// 创建上传上下文
	uploadCtx, uploadCancel := context.WithCancel(ctx)
	defer uploadCancel()
	logger.Debugf("Created upload context with cancellation support")

	logger.Infof("Starting multipart upload process for: %s", to)

	// 3. 检查目标对象是否存在且大小匹配
	logger.Debugf("Checking if target object exists: %s", to)
	head, err := s.s3Client.HeadObject(uploadCtx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(to),
	})
	logger.Debugf("HeadObject response for %s: %+v", to, head)
	// 检查目标对象是否已存在且大小匹配
	if head != nil && head.ContentLength != nil && *(head.ContentLength) == srcSize {
		atomic.AddInt64(&utils.GetProgress().SkipSize, srcSize)
		logger.Infof("Skipping upload: object %s already exists with matching size (%d bytes)", to, srcSize)
		logger.Infof("Upload completed in %v", time.Since(startTime))
		return true, nil // 大小匹配，跳过上传
	}

	// 4. 获取未完成的分段信息
	logger.Infof("Checking for incomplete uploads for: %s", to)
	uploadID, partSize, existingParts, err := s.GetIncompleteUploadRanges(ctx, to, srcSize)
	if err != nil {
		logger.Warningf("No existing upload found or error retrieving: %v", err)
		// 初始化新的分片上传
		logger.Infof("Initializing new multipart upload for: %s", to)
		id, initErr := s.InitPart(uploadCtx, s.cfg.Bucket, to)
		if initErr != nil {
			logger.Errorf("Failed to initialize multipart upload: %v", initErr)
			return false, fmt.Errorf("failed to init multipart upload: %w", initErr)
		}
		uploadID = id
		logger.Infof("Multipart upload initialized with ID: %s", uploadID)
		partSize = s.PartSize
		if partSize <= 0 {
			partSize = 32 * 1024 * 1024 // 默认 32MB
			logger.Debugf("Using default part size: %d bytes", partSize)
		} else {
			logger.Infof("Using configured part size: %d bytes", partSize)
		}
		existingParts = make([]types.Part, 0)
	} else {
		logger.Infof("Found existing incomplete upload with ID: %s", uploadID)
		logger.Infof("Found %d existing parts for %s", len(existingParts), to)
	}

	// 5. 确保在函数退出时（无论成功或失败）中止未完成的上传
	defer func() {
		if uploadID != "" {
			logger.Debugf("Aborting multipart upload on exit: %s", uploadID)
			abortCtx, cancelAbortCtx := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelAbortCtx()
			abortErr := s.AbortMultipartUpload(abortCtx, s.cfg.Bucket, to, uploadID)
			if abortErr != nil {
				logger.Errorf("Failed to abort multipart upload (ID: %s) for %s: %v", uploadID, to, abortErr)
			} else {
				logger.Infof("Aborted multipart upload (ID: %s) for %s", uploadID, to)
			}
		}
	}()

	// 6. 准备上传
	maxConcurrency := s.Concurrent
	if maxConcurrency <= 0 {
		maxConcurrency = 5 // 默认并发数
		logger.Debugf("Using default concurrency: %d", maxConcurrency)
	} else {
		logger.Infof("Using configured concurrency: %d", maxConcurrency)
	}
	logger.Infof("Preparing to upload %s (%d bytes) with part size %d and concurrency %d", to, srcSize, partSize, maxConcurrency)
	partETags := make([]types.CompletedPart, 0)
	logger.Debugf("Initialized partETags slice for storing completed parts")

	var pubWG sync.WaitGroup
	// 7. 启动生产者 Goroutine (读取数据分片)
	logger.Infof("Creating publisher channel with buffer size: %d", maxConcurrency*2)
	pubChan := make(chan PubData, maxConcurrency*2) // 缓冲 channel

	logger.Infof("Starting producer goroutine to read file parts")
	go func() {
		defer func() {
			pubWG.Wait()
			logger.Debugf("All producer workers completed, closing pubChan")
			close(pubChan)
		}()
		totalParts := (srcSize + partSize - 1) / partSize
		logger.Infof("Total parts to process: %d", totalParts)
		pubLimiter := make(chan struct{}, maxConcurrency)
		logger.Debugf("Created producer limiter with concurrency: %d", maxConcurrency)

		for pn := int64(1); pn <= totalParts; pn++ {
			// 检查上下文是否已取消
			select {
			case <-uploadCtx.Done():
				logger.Errorf("Upload context cancelled during reading part %d of %s: %v", pn, from, uploadCtx.Err())
				return
			default:
			}
			// 检查是否已上传过此分片
			partExist := false
			for _, p := range existingParts {
				if int64(aws.ToInt32(p.PartNumber)) == pn {
					partETags = append(partETags, types.CompletedPart{
						ETag:       p.ETag,
						PartNumber: p.PartNumber,
					})
					partExist = true
					atomic.AddInt64(&utils.GetProgress().SkipSize, *p.Size)
					logger.Infof("Skipping part %d of %s (already uploaded)", pn, from)
					break
				}
			}
			if partExist {
				continue
			}

			pubWG.Add(1)
			pubLimiter <- struct{}{} // 控制并发数量
			go func(idx int64) {
				defer func() {
					pubWG.Done()
					<-pubLimiter
					logger.Debugf("Released limiter for part %d publication", idx)
				}()
				logger.Debugf("starting publish worker for reading %s part %d", from, idx)
				startByte := (int64(idx) - 1) * partSize
				endByte := startByte + partSize - 1
				endByte = min(endByte, srcSize-1)
				size := endByte - startByte + 1

				// 记录分片大小信息
				if idx == totalParts {
					logger.Debugf("Last part %d of %s has adjusted size: %d bytes", idx, from, size)
				} else {
					logger.Debugf("Part %d of %s has size: %d bytes", idx, from, size)
				}

				logger.Debugf("Reading part %d of %s from offset %d to %d", idx, from, startByte, endByte)
				reader, err := fs.ReadRange(ctx, from, startByte, endByte)
				if err != nil {
					logger.Errorf("failed to read %s range [%d-%d] for part %d: %v", from, startByte, endByte, idx, err)
					uploadCancel()
					return
				} else {
					logger.Infof("success to read %s range [%d-%d] for part %d", from, startByte, endByte, idx)
				}

				buffer := make([]byte, size)
				logger.Debugf("Attempting to read %d bytes for part %d of %s", size, idx, from)
				n, readErr := io.ReadFull(reader, buffer)
				if int64(n) == size {
					logger.Debugf("Successfully read %d bytes for part %d of %s", n, idx, from)
					select {
					case <-uploadCtx.Done():
						logger.Errorf("Context cancelled while publishing part %d of %s to channel: %v", idx, from, uploadCtx.Err())
						return
					case pubChan <- PubData{
						PartNumber: idx,
						Data:       buffer,
						ReadError:  nil,
					}:
						logger.Infof("Published part %d of %s to upload channel", idx, from)
					}
					logger.Debugf("put %s part %d of %d bytes to pub chan", to, idx, size)
				} else {
					logger.Errorf("failed to read %s range data size %d:%d for part %d: %v", from, n, size, idx, err)
				}

				if readErr != nil {
					if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
						logger.Debugf("finished reading %s range data [%d-%d] for part %d", from, startByte, endByte, idx)
						return
					} else {
						logger.Errorf("error reading %s range data [%d-%d] for part %d : %v", from, startByte, endByte, idx, readErr)
						// --- 关键修改：遇到读取错误立即取消所有工作 ---
						uploadCancel() // 触发快速失败
						return         // 生产者 goroutine 退出
					}
				}
			}(pn)
		}
	}()

	// --- 8. 启动消费者协程 ---
	logger.Infof("Starting consumer goroutines for upload processing")
	retChan := make(chan RetData, maxConcurrency*2) // 结果 channel
	var cusWG sync.WaitGroup
	logger.Debugf("Created result channel with buffer size: %d", maxConcurrency*2)

	go func() {
		defer func() {
			cusWG.Wait()
			logger.Debugf("All consumer workers completed, closing result channel")
			close(retChan)
		}()
		cusLimiter := make(chan struct{}, maxConcurrency)
		logger.Debugf("Created consumer limiter with concurrency: %d", maxConcurrency)
		var workerID int32 = 0
		for p := range pubChan { // 从 channel 读取任务
			// 检查上下文是否已取消，即使在等待 channel 时
			select {
			case <-uploadCtx.Done():
				logger.Infof("Upload %s for part %d context cancelled, exiting consumer loop...", to, p.PartNumber)
				return
			default:
			}
			cusLimiter <- struct{}{} // 控制并发数量
			cusWG.Add(1)
			currentWorkerID := atomic.AddInt32(&workerID, 1)
			go func(part PubData, workerID int32) {
				defer func() {
					cusWG.Done()
					<-cusLimiter
					logger.Debugf("Worker %d: Released limiter after processing part %d", workerID, part.PartNumber)
				}()
				logger.Infof("Worker %d: Starting to process part %d of %s", workerID, part.PartNumber, to)
				// 使用 withRetry 函数处理上传重试逻辑
				etag, uploadPartErr := "", error(nil)
				attempts := 0
				uploadPartErr = utils.WithRetry(fmt.Sprintf("upload to dest %s part %d", to, part.PartNumber), s.MaxRetries, func() error {
					attempts++
					var err error
					logger.Debugf("Worker %d: Attempt %d to upload part %d (%d bytes)", workerID, attempts, part.PartNumber, len(part.Data))
					etag, err = s.UploadPart(uploadCtx, s.cfg.Bucket, to, uploadID, part.PartNumber, part.Data)
					if err == nil {
						logger.Infof("Worker %d: Successfully uploaded part %d in %d attempt(s)", workerID, part.PartNumber, attempts)
					} else {
						logger.Warningf("Worker %d: Failed to upload part %d (attempt %d/%d): %v", workerID, part.PartNumber, attempts, s.MaxRetries, err)
					}
					return err
				})

				// --- 关键：如果上传失败，立即取消所有工作 ---
				if uploadPartErr != nil {
					logger.Errorf("Worker %d: Upload error for part %d of %s: %v", workerID, part.PartNumber, to, uploadPartErr)
					uploadCancel() // 触发取消
				} else {
					// 将结果发送回主 goroutine
					logger.Debugf("Worker %d: Sending successful upload result for part %d to result channel", workerID, part.PartNumber)
					retChan <- RetData{
						PartNumber: part.PartNumber,
						ETag:       etag,
						Error:      nil,
					}
				}

				logger.Infof("Worker %d: Finished processing part %d of %s", workerID, part.PartNumber, to)
			}(p, currentWorkerID)
		}
		logger.Infof("Consumer loop completed - no more parts to process")
	}()

	// 10. 收集并处理上传结果 (在主 Goroutine 中) ---
	logger.Infof("Starting to collect and process upload results for %s", to)
	var failedParts int64

	for ret := range retChan {
		logger.Debugf("Received upload result for part %d of %s", ret.PartNumber, to)
		if ret.Error != nil {
			failedParts++
			logger.Errorf("Failed to upload part %d of %s: %v", ret.PartNumber, to, ret.Error)
		} else {
			partETags = append(partETags, types.CompletedPart{
				ETag:       aws.String(ret.ETag),
				PartNumber: aws.Int32(int32(ret.PartNumber)),
			})
			logger.Infof("Successfully processed part %d of %s", ret.PartNumber, to)
		}
	}

	// 检查是否有失败的分片
	logger.Infof("Upload result summary for %s: %d parts succeeded, %d parts failed", to, len(partETags), failedParts)
	if failedParts > 0 {
		logger.Errorf("Upload of %s failed with %d parts failed", to, failedParts)
		return false, fmt.Errorf("upload of %s failed with %d parts failed", to, failedParts)
	}

	// 11. 检查是否出错 ---
	if uploadCtx.Err() != nil {
		logger.Errorf("Upload context for %s was cancelled: %v", to, uploadCtx.Err())
		return false, fmt.Errorf("upload failed: %s", to)
	}

	// 12. 收集并排序分片结果（按 PartNumber） ---
	sort.Slice(partETags, func(i, j int) bool {
		return *partETags[i].PartNumber < *partETags[j].PartNumber
	})

	// 13. 完成分片上传 ---
	logger.Infof("All parts uploaded, starting to complete multipart upload for %s", to)
	logger.Debugf("Calling CompletePart for %s (upload ID: %s)", to, uploadID)
	completeErr := s.CompletePart(uploadCtx, s.cfg.Bucket, to, uploadID, partETags)
	if completeErr != nil {
		logger.Errorf("Failed to complete multipart upload for %s: %v", to, completeErr)
		return false, fmt.Errorf("failed to complete multipart upload for %s: %w", to, completeErr)
	}

	// 14. 成功完成 ---
	logger.Infof("successfully completed multipart upload for %s with %d parts", to, len(partETags))
	logger.Infof("Successfully uploaded %s to %s", from, to)
	uploadID = "" // 防止 defer 中执行 Abort
	return true, nil
}

func (s *S3Cli) HeadMultipartUpload(ctx context.Context, objectPath string) (*types.MultipartUpload, error) {
	params := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.cfg.Bucket),
		Prefix: aws.String(objectPath),
	}

	var latest *types.MultipartUpload

	paginator := s3.NewListMultipartUploadsPaginator(s.s3Client, params)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
		}

		for _, u := range page.Uploads {
			if *u.Key == objectPath {
				if latest == nil || u.Initiated.After(*latest.Initiated) {
					latest = &u
				}
			}
		}
	}

	if latest == nil {
		return nil, nil // No matching upload found
	}

	// Check if more than 24 hours have passed
	now := time.Now()
	duration := now.Sub(*latest.Initiated)
	if duration > 24*time.Hour {
		return nil, nil // Exceeded 24 hours, considered invalid, not returning
	}

	return latest, nil
}

// ListParts 获取当前上传的所有已上传分片
func (s *S3Cli) ListParts(ctx context.Context, bucketName, objectKey, uploadID string) ([]types.Part, error) {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return nil, errors.New("input params is empty")
	}

	var allParts []types.Part
	var partNumberMarker *string
	const maxParts = 10000
	for {
		resp, err := s.s3Client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           aws.String(bucketName),
			Key:              aws.String(objectKey),
			UploadId:         aws.String(uploadID),
			MaxParts:         aws.Int32(maxParts),
			PartNumberMarker: partNumberMarker,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list parts: %w", err)
		}

		allParts = append(allParts, resp.Parts...)

		if *resp.IsTruncated && resp.NextPartNumberMarker != nil {
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

	logger.Infof("initializing multipart upload for %s/%s", bucketName, objectKey)

	resp, err := s.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
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

	logger.Infof("start uploading %d bytes to part %d of %s/%s", len(data), partNumber, bucketName, objectKey)

	resp, err := s.s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int32(int32(partNumber)),
		Body:       bytes.NewReader(data),
		UploadId:   aws.String(uploadID),
	})
	if err != nil {
		logger.Errorf("upload part %d of %s/%s failed: %s", partNumber, bucketName, objectKey, err)
		return "", fmt.Errorf("failed to upload part %d: %w", partNumber, err)
	}

	atomic.AddInt64(&utils.GetProgress().UploadSize, int64(len(data)))
	logger.Infof("successfully uploaded %s/%s part %d", bucketName, objectKey, partNumber)
	return *resp.ETag, nil
}

// CompletePart 完成分片上传
func (s *S3Cli) CompletePart(ctx context.Context, bucketName, objectKey, uploadID string, uploadedParts []types.CompletedPart) error {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return errors.New("input params is empty")
	}

	// 排序 Parts
	sortedParts := make([]types.CompletedPart, len(uploadedParts))
	copy(sortedParts, uploadedParts)
	for i := range sortedParts {
		for j := i + 1; j < len(sortedParts); j++ {
			if aws.ToInt32(sortedParts[i].PartNumber) > aws.ToInt32(sortedParts[j].PartNumber) {
				sortedParts[i], sortedParts[j] = sortedParts[j], sortedParts[i]
			}
		}
	}

	multipartUpload := types.CompletedMultipartUpload{
		Parts: sortedParts,
	}

	logger.Infof("completing multipart upload for %s/%s", bucketName, objectKey)

	_, err := s.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(objectKey),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &multipartUpload,
	})

	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	logger.Infof("completed multipart upload for %s/%s", bucketName, objectKey)
	return nil
}

// AbortMultipartUpload 中止指定的分片上传任务
func (s *S3Cli) AbortMultipartUpload(ctx context.Context, bucketName, objectKey, uploadID string) error {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return errors.New("input params is empty")
	}

	logger.Infof("aborting multipart upload: UploadId=%s", uploadID)

	_, err := s.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	logger.Infof("successfully aborted multipart upload: UploadId=%s", uploadID)
	return nil
}

// CanUseCopyObject 判断是否可以使用CopyObject方法
func (s *S3Cli) CanUseCopyObject(_ context.Context, fromEp, toEp *source.EndpointConfig) bool {
	// Check if same origin (same account and region)
	isSameOrigin := fromEp.Region == toEp.Region && fromEp.AccessKey == toEp.AccessKey
	return isSameOrigin
}

// CopyObject 复制对象从源路径到目标路径（仅支持同一账户和区域）
func (s *S3Cli) CopyObject(ctx context.Context, fromEp *source.EndpointConfig, fromKey, toKey string, srcmeta map[string]string) (bool, error) {
	// 检查目标对象是否已存在且ETag匹配
	var srcEtag string
	if srcmeta != nil {
		srcEtag = srcmeta["etag"]
	}

	if srcEtag != "" {
		exist, _, err := s.IsObjectExist(ctx, toKey, srcEtag)
		if err == nil && exist {
			logger.Infof("object %s already exists with matching etag, skipping copy object", toKey)
			return true, nil
		}
	}

	// 创建复制源路径（格式：bucket/key）
	copySrc := fmt.Sprintf("%s/%s", fromEp.Bucket, fromKey)

	// 准备复制对象请求
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(s.cfg.Bucket),
		Key:        aws.String(toKey),
		CopySource: aws.String(copySrc),
	}

	// 添加元数据
	if len(srcmeta) > 0 {
		metadataMap := make(map[string]string)
		for key, value := range srcmeta {
			metadataMap[key] = value
		}
		params.Metadata = metadataMap
		params.MetadataDirective = types.MetadataDirectiveReplace
	}

	logger.Infof("copying object from %s/%s to %s/%s", fromEp.Bucket, fromKey, s.cfg.Bucket, toKey)

	// 执行复制操作
	return false, utils.WithRetry(fmt.Sprintf("copy to dest object %s", toKey), s.MaxRetries, func() error {
		_, err := s.s3Client.CopyObject(ctx, params)
		if err == nil {
			logger.Infof("successfully copied object %s/%s to %s/%s",
				fromEp.Bucket, fromKey, s.cfg.Bucket, toKey)
		}
		return err
	})
}

// IsUploadIDExist 检查指定的上传ID是否存在
func (s *S3Cli) IsUploadIDExist(ctx context.Context, bucket, key, uploadID string) (bool, error) {
	if bucket == "" || key == "" || uploadID == "" {
		return false, errors.New("input params is empty")
	}

	_, err := s.s3Client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MaxParts: aws.Int32(1),
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

// IsBucketExist 检查指定的 S3 Bucket 是否存在。
func (s *S3Cli) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err := s.s3Client.HeadBucket(ctx, input)
	if err != nil {
		var apiErr *types.NotFound
		if errors.As(err, &apiErr) {
			// Bucket 不存在
			return false, nil
		}
		// 其他错误（如权限不足、网络问题等）
		return false, err
	}

	// 没有出错，说明 Bucket 存在
	return true, nil
}

// CreateBucket 创建一个新 Bucket
func (s *S3Cli) CreateBucket(ctx context.Context, bucketName string) error {
	logger.Infof("creating bucket: %s", bucketName)

	_, err := s.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		var apiErr *types.BucketAlreadyOwnedByYou
		if errors.As(err, &apiErr) {
			logger.Infof("bucket %s already owned by you.", bucketName)
			return nil
		}
		var apiErr2 *types.BucketAlreadyExists
		if errors.As(err, &apiErr2) {
			logger.Infof("bucket %s already exists but not owned by you.", bucketName)
			return nil
		}
		logger.Infof("failed to create bucket %s: %v", bucketName, err)
		return err
	}

	logger.Infof("bucket created successfully: %s", bucketName)
	return nil
}

// GetIncompleteUploadRanges 计算未完成上传的区间集合
func (s *S3Cli) GetIncompleteUploadRanges(ctx context.Context, objectPath string, objectSize int64) (string, int64, []types.Part, error) {
	// 1. Get incomplete upload task
	upload, err := s.HeadMultipartUpload(ctx, objectPath)
	if err != nil {
		return "", 0, nil, fmt.Errorf("failed to get multipart upload: %w", err)
	}
	if upload == nil {
		return "", 0, nil, errors.New("no incomplete multipart upload found")
	}
	uploadID := *upload.UploadId
	bucketName := s.cfg.Bucket
	objectKey := objectPath

	// 2. Get uploaded parts
	parts, err := s.ListParts(ctx, bucketName, objectKey, uploadID)
	if err != nil {
		return "", 0, nil, fmt.Errorf("failed to list parts: %w", err)
	}

	// 3. Calculate old part sizes count
	var partSizeList []int64
	for _, part := range parts {
		partSizeList = append(partSizeList, aws.ToInt64(part.Size))
	}
	partSizeList = utils.Unique(partSizeList)
	// Sort (optional)
	sort.Slice(partSizeList, func(i, j int) bool {
		return partSizeList[i] < partSizeList[j]
	})
	// Should have only 1 or 2 split lengths
	if len(partSizeList) < 1 || len(partSizeList) > 2 {
		logger.Warningf("failed to continue upload for %s uploadID %s parts %v", objectPath, uploadID, partSizeList)
		return "", 0, nil, fmt.Errorf("failed to continue upload for %s uploadID %s parts %v", objectPath, uploadID, parts)
	}
	logger.Infof("successfully retrieved incomplete multipart upload for %s UploadId=%s %d part %v ", objectPath, uploadID, len(parts), partSizeList)
	// 4. Calculate total number of parts
	partSize := partSizeList[0]

	totalParts := int64(math.Ceil(float64(objectSize) / float64(partSize)))
	// Verify if the last part size is correct
	if totalParts < 2 || (len(partSizeList) > 1 && partSizeList[1] != (objectSize-partSize*totalParts-1)) {
		logger.Warningf("part num or part size not match for %s", objectPath)
		return "", 0, nil, fmt.Errorf("part num or part size not match for %s", objectPath)
	}
	for _, part := range parts {
		if int64(aws.ToInt32(part.PartNumber)) > totalParts {
			logger.Warningf("%s part num %d exceed the total part num %d", objectPath, aws.ToInt32(part.PartNumber), totalParts)
			return "", 0, nil, fmt.Errorf("%s part num %d exceed the total part num %d", objectPath, aws.ToInt32(part.PartNumber), totalParts)
		}
	}

	return uploadID, partSize, parts, nil
}
