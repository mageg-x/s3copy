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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"io"
	"math"
	"s3copy/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	head, err := s.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(objectPath),
	})
	if err != nil {
		logger.Infof("failed to check if object exists: %v", err)
		return false, "", fmt.Errorf("failed to check if object exists: %w", err)
	}

	dstEtag := ""
	// 如果对象存在且 ETag 匹配，直接返回成功
	if head != nil && head.ETag != nil {
		dstEtag = *head.ETag
	}
	if strings.Trim(srcEtag, `"`) == strings.Trim(dstEtag, `"`) {
		logger.Infof("object %s already exists with matching etag (%s), skipping upload", objectPath, dstEtag)
		return true, dstEtag, nil
	}
	logger.Infof("etag %s not matching etag (%s)", srcEtag, head)
	return false, dstEtag, errors.New("etag not match")
}

// uploadSimple 执行简单上传
func (s *S3Cli) UploadObject(ctx context.Context, fs source.Source, from, to string, srcmeta map[string]string) error {
	reader, _, err := fs.Read(ctx, from, 0)
	if err != nil {
		logger.Errorf("failed to read %s: %v", from, err)
		return fmt.Errorf("failed to open source %s", from)
	}
	defer reader.Close()

	var srcEtag, dstEtag string
	if srcmeta != nil {
		srcEtag = srcmeta["etag"]
	}

	if srcEtag != "" {
		exist, e, err := s.IsObjectExist(ctx, to, srcEtag)
		dstEtag = e
		if err == nil && exist {
			return nil
		}
	}

	// 将流式数据读入内存
	buf, err := io.ReadAll(reader)
	if err != nil {
		logger.Errorf("failed to read object %s: %v", to, err)
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

	exist, dstEtag, err := s.IsObjectExist(ctx, to, srcEtag)
	if err == nil && exist {
		return nil
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(to),
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

	for attempt := 0; attempt < s.MaxRetries; attempt++ {
		// 执行上传
		_, err = s.s3Client.PutObjectWithContext(ctx, params)
		if err != nil {
			logger.Errorf("failed to upload object %s: %v", to, err)
		} else {
			return nil
		}
		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("failed to upload object %s: %w", to, err)
}

// UploadMultipart 上传文件到 S3，使用分片并行上传方式，并支持快速失败。
func (s *S3Cli) UploadMultipart(ctx context.Context, fs source.Source, from, to string, srcmeta map[string]string) error {
	reader, _, err := fs.Read(ctx, from, 0)
	if err != nil {
		logger.Errorf("failed to read %s: %v", from, err)
		return fmt.Errorf("failed to open source %s", from)
	}
	defer reader.Close()
	// 1. 解析源文件大小
	srcSizeStr, ok := srcmeta["size"]
	if !ok {
		return fmt.Errorf("missing source size in metadata")
	}
	srcSize, err := strconv.ParseInt(srcSizeStr, 10, 64)
	if err != nil || srcSize <= 0 {
		return fmt.Errorf("invalid source size in metadata: %v", err)
	}

	uploadCtx, uploadCancel := context.WithCancel(ctx)
	defer uploadCancel()

	logger.Infof("starting multipart upload for %s", to)

	// 3. 检查目标对象是否存在且大小匹配 (简化)
	head, err := s.s3Client.HeadObjectWithContext(uploadCtx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(to),
	})

	// 如果etag中含有-,说明是分段上传的
	if head != nil && *(head.ContentLength) == srcSize {
		atomic.AddInt64(&utils.GetProgress().UploadSize, srcSize)
		logger.Infof("skip object %s already exist src : %v  dest %v", to, srcmeta, head)
		return nil // 大小匹配，跳过上传
	}

	// 4、获取未完成的分段信息
	uploadID, partSize, existingParts, err := s.GetIncompleteUploadRanges(ctx, to, srcSize)
	if err != nil {
		// 4. 初始化分片上传
		id, initErr := s.InitPart(uploadCtx, s.cfg.Bucket, to)
		if initErr != nil {
			return fmt.Errorf("failed to init multipart upload: %w", initErr)
		}
		logger.Infof("initialized multipart upload for %s with ID: %s", to, uploadID)
		uploadID = id
		partSize = s.PartSize
		if partSize <= 0 {
			partSize = 32 * 1024 * 1024 // 默认 5MB
		}
		existingParts = make([]*s3.Part, 0)
	}

	// 5. 确保在函数退出时（无论成功或失败）中止上传（如果 uploadID 仍然存在）
	defer func() {
		if uploadID != "" {
			abortCtx, cancelAbortCtx := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelAbortCtx()
			abortErr := s.AbortMultipartUpload(abortCtx, s.cfg.Bucket, to, uploadID)
			if abortErr != nil {
				logger.Errorf("failed to abort multipart upload (ID: %s) for %s: %v", uploadID, to, abortErr)
			} else {
				logger.Infof("aborted multipart upload (ID: %s) for %s", uploadID, to)
			}
		}
	}()

	// 6. 准备上传
	maxConcurrency := s.Concurrent
	if maxConcurrency <= 0 {
		maxConcurrency = 5 // 默认并发数
	}
	logger.Infof("preparing to upload %s (%d bytes), part size %d, concurrency %d", to, srcSize, partSize, maxConcurrency)
	partETags := make([]*s3.CompletedPart, 0)

	var pubWG sync.WaitGroup
	// 7. 启动生产者 Goroutine (写入数据)
	pubChan := make(chan PubData, maxConcurrency*2) // 缓冲 channel
	go func() {
		defer func() {
			pubWG.Wait()
			close(pubChan)
		}()
		totalParts := (srcSize + partSize - 1) / partSize
		pubLimiter := make(chan struct{}, maxConcurrency)

		for pn := int64(1); pn <= totalParts; pn++ {
			// 检查父上下文或工作上下文是否已取消
			select {
			case <-uploadCtx.Done():
				logger.Errorf("upload %s  for part %d context cancelled during reading: %v", from, pn, uploadCtx.Err())
				return
			default:
			}
			// 检查是否上传过
			partExist := false
			for _, p := range existingParts {
				if *p.PartNumber == pn {
					partETags = append(partETags, &s3.CompletedPart{
						ETag:       aws.String(*p.ETag),
						PartNumber: aws.Int64(*p.PartNumber),
					})
					partExist = true
					atomic.AddInt64(&utils.GetProgress().UploadSize, *p.Size)
					break
				}
			}
			if partExist {

				logger.Infof("found %s  for part %d already exists  then skip upload", from, pn)
				continue
			}

			pubWG.Add(1)
			pubLimiter <- struct{}{} // 控制并发数量
			go func(idx int64) {
				defer func() {
					pubWG.Done()
					<-pubLimiter
				}()
				logger.Infof("starting publish worker for reading %s part %d", from, idx)
				startByte := (int64(idx) - 1) * partSize
				endByte := startByte + partSize - 1
				endByte = min(endByte, srcSize-1)
				size := endByte - startByte + 1
				reader, err := fs.ReadRange(ctx, from, startByte, endByte)
				if err != nil {
					logger.Errorf("failed to read %s range [%d-%d] for part %d: %v", from, startByte, endByte, idx, err)
					uploadCancel()
					return
				} else {
					logger.Infof("success to read %s range [%d-%d] for part %d", from, startByte, endByte, idx)
				}

				buffer := make([]byte, size)
				n, readErr := io.ReadFull(reader, buffer)
				if int64(n) == size {
					pubChan <- PubData{
						PartNumber: idx,
						Data:       buffer,
						ReadError:  nil,
					}
					logger.Infof("put %s part %d of %d bytes to pub chan", to, idx, size)
				} else {
					logger.Errorf("failed to read %s range data size %d:%d for part %d: %v", from, n, size, idx, err)
				}

				if readErr != nil {
					if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
						logger.Infof("finished reading %s range data [%d-%d] for part %d", from, startByte, endByte, idx)
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
	retChan := make(chan RetData, maxConcurrency*2) // 结果 channel
	var cusWG sync.WaitGroup

	go func() {
		defer func() {
			cusWG.Wait()
			close(retChan)
		}()
		cusLimiter := make(chan struct{}, maxConcurrency)
		for p := range pubChan { // 从 channel 读取任务
			// 检查上下文是否已取消，即使在等待 channel 时
			select {
			case <-uploadCtx.Done():
				logger.Infof("upload %s for part %d context cancelled, exiting...", to, p.PartNumber)
				return
			default:
			}
			cusLimiter <- struct{}{} // 控制并发数量
			cusWG.Add(1)
			go func(part PubData) {
				defer func() {
					cusWG.Done()
					<-cusLimiter
				}()
				logger.Infof("starting upload %s part %d", to, part.PartNumber)
				// 执行上传重试逻辑
				var etag string
				var uploadPartErr error
				for attempt := 0; attempt < s.MaxRetries; attempt++ {
					// 使用 partCtx，它会响应 workCtx 的取消
					etag, uploadPartErr = s.UploadPart(uploadCtx, s.cfg.Bucket, to, uploadID, part.PartNumber, part.Data)
					if uploadPartErr == nil {
						logger.Infof("successfully uploaded %s for part %d (%d bytes)", to, part.PartNumber, len(part.Data))
						break
					}
					logger.Errorf("failed to upload  %s for part %d (attempt %d/%d): %v", to, part.PartNumber, attempt+1, s.MaxRetries, uploadPartErr)
					time.Sleep(3 * time.Second)
				}

				// --- 关键：如果上传失败，立即取消所有工作 ---
				if uploadPartErr != nil {
					logger.Errorf("upload  %s  error for part %d", to, part.PartNumber)
					uploadCancel() // 触发取消
				} else {
					// 将结果发送回主 goroutine
					retChan <- RetData{
						PartNumber: part.PartNumber,
						ETag:       etag,
						Error:      nil,
					}
				}

				logger.Infof("upload %s for part %d finished", to, part.PartNumber)
			}(p)
		}
	}()

	// 10. 收集并处理上传结果 (在主 Goroutine 中) ---

	for ret := range retChan {
		partETags = append(partETags, &s3.CompletedPart{
			ETag:       aws.String(ret.ETag),
			PartNumber: aws.Int64(ret.PartNumber),
		})
	}

	// 11. 检查是否出错 ---
	if uploadCtx.Err() != nil {
		return fmt.Errorf("uploaded failed for %s", to)
	}

	// 12. 收集并排序分片结果（按 PartNumber） ---
	sort.Slice(partETags, func(i, j int) bool {
		return *partETags[i].PartNumber < *partETags[j].PartNumber
	})

	// 13. 完成分片上传 ---
	completeErr := s.CompletePart(uploadCtx, s.cfg.Bucket, to, uploadID, partETags)
	if completeErr != nil {
		return fmt.Errorf("failed to complete multipart upload for %s: %w", to, completeErr)
	}

	// 14. 成功完成 ---
	logger.Infof("successfully completed multipart upload for %s with %d parts", to, len(partETags))
	uploadID = "" // 防止 defer 中执行 Abort
	return nil
}

func (s *S3Cli) HeadMultipartUpload(ctx context.Context, objectPath string) (*s3.MultipartUpload, error) {
	params := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.cfg.Bucket),
		Prefix: aws.String(objectPath),
	}

	var latest *s3.MultipartUpload

	err := s.s3Client.ListMultipartUploadsPagesWithContext(ctx, params,
		func(page *s3.ListMultipartUploadsOutput, lastPage bool) bool {
			for _, u := range page.Uploads {
				if *u.Key == objectPath {
					if latest == nil || u.Initiated.After(*latest.Initiated) {
						latest = u
					}
				}
			}
			return true
		})

	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	if latest == nil {
		return nil, nil // 未找到匹配的上传
	}

	// 检查是否超过 24 小时
	now := time.Now()
	duration := now.Sub(*latest.Initiated)
	if duration > 24*time.Hour {
		return nil, nil // 超过 24 小时，视为无效，不返回
	}

	return latest, nil
}

// ListParts 获取当前上传的所有已上传分片
func (s *S3Cli) ListParts(ctx context.Context, bucketName, objectKey, uploadID string) ([]*s3.Part, error) {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return nil, errors.New("input params is empty")
	}

	var allParts []*s3.Part
	var partNumberMarker *int64 = nil
	const maxParts = 10000
	for {
		resp, err := s.s3Client.ListPartsWithContext(ctx, &s3.ListPartsInput{
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

	logger.Infof("initializing multipart upload for %s/%s", bucketName, objectKey)

	resp, err := s.s3Client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
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

	resp, err := s.s3Client.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(partNumber),
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
func (s *S3Cli) CompletePart(ctx context.Context, bucketName, objectKey, uploadID string, uploadedParts []*s3.CompletedPart) error {
	if bucketName == "" || objectKey == "" || uploadID == "" {
		return errors.New("input params is empty")
	}

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

	logger.Infof("completing multipart upload for %s/%s", bucketName, objectKey)

	_, err := s.s3Client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
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

	_, err := s.s3Client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
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

// IsUploadIDExist 检查指定的上传ID是否存在
func (s *S3Cli) IsUploadIDExist(ctx context.Context, bucket, key, uploadID string) (bool, error) {
	if bucket == "" || key == "" || uploadID == "" {
		return false, errors.New("input params is empty")
	}

	_, err := s.s3Client.ListPartsWithContext(ctx, &s3.ListPartsInput{
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

// IsBucketExist 检查指定的 S3 Bucket 是否存在。
func (s *S3Cli) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err := s.s3Client.HeadBucketWithContext(ctx, input)
	if err != nil {
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
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

	_, err := s.s3Client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "BucketAlreadyOwnedByYou":
				logger.Infof("bucket %s already owned by you.", bucketName)
				return nil
			case "BucketAlreadyExists":
				logger.Infof("bucket %s already exists but not owned by you.", bucketName)
				return nil
				// return fmt.Errorf("bucket %s already exists but not owned by you", bucketName)
			default:
				logger.Infof("failed to create bucket %s: %v", bucketName, err)
				return err
			}
		} else {
			logger.Infof("Failed to create bucket %s: %v", bucketName, err)
			return err
		}
	}

	logger.Infof("bucket created success : %s", bucketName)
	return nil
}

// GetIncompleteUploadRanges 计算未完成上传的区间集合
func (s *S3Cli) GetIncompleteUploadRanges(ctx context.Context, objectPath string, objectSize int64) (string, int64, []*s3.Part, error) {
	// 1. 获取未完成的上传任务
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

	// 2. 获取已上传的分片
	parts, err := s.ListParts(ctx, bucketName, objectKey, uploadID)
	if err != nil {
		return "", 0, nil, fmt.Errorf("failed to list parts: %w", err)
	}

	// 3、计算旧的分片大小个数
	var partSizeList []int64
	for _, part := range parts {
		partSizeList = append(partSizeList, *part.Size)
	}
	partSizeList = utils.Unique(partSizeList)
	// 排序（可选）
	sort.Slice(partSizeList, func(i, j int) bool {
		return partSizeList[i] < partSizeList[j]
	})
	//应该只有 1或者2种切分长度
	if len(partSizeList) < 1 || len(partSizeList) > 2 {
		logger.Infof("failed to continue upload for %s uploadID %s parts %v", objectPath, uploadID, partSizeList)
		return "", 0, nil, fmt.Errorf("failed to continue upload for %s uploadID %s parts %v", objectPath, uploadID, parts)
	}
	logger.Infof("successfully retrieved incomplete multipart upload for %s UploadId=%s %d part %v ", objectPath, uploadID, len(parts), partSizeList)
	// 4. 计算总分片数
	partSize := partSizeList[0]

	totalParts := int64(math.Ceil(float64(objectSize) / float64(partSize)))
	//校验最后一个分片大小是否正确
	if totalParts < 2 || (len(partSizeList) > 1 && partSizeList[1] != (objectSize-partSize*(totalParts-1))) {
		logger.Errorf("part num or part size not match for %s", objectPath)
		return "", 0, nil, fmt.Errorf("part num or part size not match for %s", objectPath)
	}
	for _, part := range parts {
		if *part.PartNumber > totalParts {
			logger.Errorf("%s part num %d exceed the total part num %d", objectPath, *part.PartNumber, totalParts)
			return "", 0, nil, fmt.Errorf("%s part num %d exceed the total part num %d", objectPath, *part.PartNumber, totalParts)
		}
	}

	return uploadID, partSize, parts, nil
}
