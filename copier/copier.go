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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	source "s3copy/filesource"
	"s3copy/utils"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Copier 处理从文件源到S3的复制
type Copier struct {
	copyOpt    *CopyOptions
	destConfig *source.EndpointConfig
}

// CopyOptions 包含复制操作的选项
type CopyOptions struct {
	SourcePath string // 源路径
	SourceType string // 源类型: file, http, s3
	DestPath   string // 目标路径
	DestType   string // 目标类型: 固定 s3
	Concurrent int    // 并发上传数量
	PartSize   int64  // 传输分块大小
}

var logger = utils.GetLogger("s3copy")

// NewCopier 创建文件复制器实例
func NewCopier(opt *CopyOptions) (*Copier, error) {
	if opt == nil {
		logger.Errorf("copy option is required")
		return nil, errors.New("copy option is required")
	}
	destConfig, err := source.ParseEndpoint(opt.DestPath, true)
	if err != nil {
		logger.Errorf("failed to parse destination endpoint: %v", err)
		return nil, fmt.Errorf("failed to parse dest s3 path %s", opt.DestPath)
	}
	logger.Debugf("copy option: %+v, destConfig :%+v", opt, destConfig)
	if destConfig.Prefix != "" || destConfig.Bucket == "" {
		logger.Errorf("dest s3 path %s is invalid, must a bucket address ", opt.DestPath)
		return nil, fmt.Errorf("dest s3 path %s is invalid, must a bucket address ", opt.DestPath)
	}
	return &Copier{
		copyOpt:    opt,
		destConfig: destConfig,
	}, nil
}

func (c *Copier) Stat(src source.Source) {
	progress := utils.GetProgress()
	fileCh, _ := src.List(context.Background(), true)
	for f := range fileCh {
		if f.IsDir {
			continue
		}
		atomic.AddInt64(&progress.TotalSize, f.Size)
		atomic.AddInt64(&progress.TotalObjects, 1)
	}
}

func (c *Copier) Copy() error {
	logger.Infof("Starting copy operation: source=%s (%s) to %s (%s)",
		c.copyOpt.SourcePath, c.copyOpt.SourceType,
		c.copyOpt.DestPath, c.copyOpt.DestType)
	startTime := time.Now()

	// 打开上传
	logger.Debugf("Creating S3 client with partSize=%d, concurrent=%d", c.copyOpt.PartSize, c.copyOpt.Concurrent)
	s3cli, err := Create(c.destConfig, 3, int(c.copyOpt.PartSize), c.copyOpt.Concurrent)
	if err != nil {
		logger.Errorf("Failed to create S3 client: %v", err)
		return err
	}
	logger.Infof("S3 client created successfully")

	// --- 使用 context 控制取消 ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在 Copy 函数结束时取消 context
	logger.Debugf("Created context with cancellation support")

	// 如果目标bucket不存在就创建
	logger.Debugf("Checking if destination bucket exists: %s", c.destConfig.Bucket)
	if ok, err := s3cli.IsBucketExist(ctx, c.destConfig.Bucket); err != nil || !ok {
		logger.Infof("Destination bucket does not exist, creating: %s", c.destConfig.Bucket)
		_ = s3cli.CreateBucket(ctx, c.destConfig.Bucket)
	}

	if ok, err := s3cli.IsBucketExist(ctx, c.destConfig.Bucket); err != nil || !ok {
		logger.Errorf("Failed to create destination bucket: %s", c.destConfig.Bucket)
		return errors.New("dst bucket does not exist")
	}
	logger.Infof("Destination bucket is ready: %s", c.destConfig.Bucket)

	// 判断是否同源，可以使用copyObject
	isSameOrigin := false
	var srcEndPoint *source.EndpointConfig
	if c.copyOpt.SourceType == "s3" {
		logger.Debugf("Source is S3, checking if same origin")
		ep, err := source.ParseEndpoint(c.copyOpt.SourcePath, false)
		srcEndPoint = ep
		if err == nil && srcEndPoint != nil {
			isSameOrigin = s3cli.CanUseCopyObject(ctx, srcEndPoint, c.destConfig)
			logger.Infof("Same origin check result: %v", isSameOrigin)
		} else {
			logger.Errorf("Failed to parse source endpoint: %v", err)
		}
	}

	// 创建文件源
	logger.Infof("Creating source of type: %s, path: %s", c.copyOpt.SourceType, c.copyOpt.SourcePath)
	srcFS, err := source.NewSource(c.copyOpt.SourceType, c.copyOpt.SourcePath)
	if err != nil {
		logger.Fatalf("Failed to create source: %v", err)
	}
	logger.Infof("Source created successfully")

	// 为了统计需要，要知道文件总个数和总大小
	logger.Debugf("Starting file statistics collection")
	go c.Stat(srcFS)
	logger.Debugf("Starting progress reporter")
	go utils.StartProgressReporter(context.Background(), utils.GetProgress())

	// 获取文件列表
	logger.Infof("Listing files from source")
	fileCh, errCh := srcFS.List(ctx, true)

	// --- 主协程：监听 errCh ---
	go func() {
		select {
		case err := <-errCh:
			if err != nil {
				logger.Errorf("Failed to list files: %v", err)
				cancel()
			}
		case <-ctx.Done(): // 如果主 context 已结束，就不处理了
			logger.Infof("Context cancelled, stopping error listener")
			return
		}
	}()

	// 控制并发数量
	concurrency := 10
	if c.copyOpt.Concurrent > 0 {
		concurrency = c.copyOpt.Concurrent
	}
	logger.Infof("Setting concurrency level to: %d", concurrency)

	// 创建任务通道
	logger.Debugf("Creating task channel with buffer size: 100")
	taskCh := make(chan source.ObjectInfo, 100)
	var wg sync.WaitGroup

	// 消费者goroutine
	logger.Infof("Starting %d worker goroutines", concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logger.Debugf("Worker %d started", workerID)
			for f := range taskCh {
				// 检查 context 是否已取消（由其他 worker 或主协程触发）
				select {
				case <-ctx.Done():
					logger.Infof("Worker %d: context cancelled, stopping worker.", workerID)
					return // 退出当前 worker
				default:
				}
				logger.Debugf("Worker %d: preparing to upload file: %s", workerID, f.Key)
				logger.Tracef("Worker %d: file info: %+v", workerID, f)
				if f.IsDir {
					logger.Debugf("Worker %d: skipping directory: %s", workerID, f.Key)
					continue
				}
				// 1. 获取对象元数据
				logger.Infof("Worker %d: getting metadata for: %s", workerID, f.Key)
				objMeta, err := srcFS.GetMetadata(ctx, f.Key)
				if err != nil {
					logger.Errorf("Worker %d: failed to get metadata for %s: %v", workerID, f.Key, err)
					continue
				}
				logger.Debugf("Worker %d: got metadata for %s: %v", workerID, f.Key, objMeta)

				// 2. 确定对象大小
				objSize := f.Size
				// 处理目标key
				key := f.Key
				logger.Debugf("Worker %d: original key: %s", workerID, key)
				switch c.copyOpt.SourceType {
				case "file":
					// 假设 sourcePath 是类似 "/data/input/" 或 "/data/input" 的路径
					prefix := filepath.Dir(c.copyOpt.SourcePath)
					// 去掉前缀
					key = strings.TrimPrefix(key, prefix)
					// 去掉开头和结尾的 '/'，避免出现 "/project/file.txt" 或 "project/file.txt/"
					key = strings.Trim(key, "/")
					logger.Debugf("Worker %d: processed file key: %s", workerID, key)
				case "http":
					logger.Debugf("Worker %d: getting file name from URL: %s", workerID, c.copyOpt.SourcePath)
					key, err = utils.GetFileNameFromURL(c.copyOpt.SourcePath)
					if err != nil {
						logger.Errorf("Worker %d: failed to get file name from URL: %v", workerID, err)
						continue
					}
					logger.Debugf("Worker %d: processed HTTP key: %s", workerID, key)
				default:
					logger.Debugf("Worker %d: using original key for source type: %s", workerID, c.copyOpt.SourceType)
				}

				logger.Infof("Worker %d: copying from %s to %s object size %d", workerID, f.Key, key, objSize)
				progress := utils.GetProgress()
				var uploadErr error
				var skip bool
				if isSameOrigin {
					logger.Infof("Worker %d: same origin detected, using CopyObject for %s", workerID, key)
					skip, uploadErr = s3cli.CopyObject(ctx, srcEndPoint, f.Key, key, objMeta)
					if uploadErr != nil {
						atomic.AddInt64(&progress.FailObjects, 1)
						logger.Errorf("Worker %d: failed to copy object %s: %v", workerID, key, uploadErr)
					} else {
						if !skip {
							atomic.AddInt64(&progress.UploadObjects, 1)
							atomic.AddInt64(&progress.UploadSize, objSize)
							logger.Infof("Worker %d: successfully copied object: %s", workerID, key)
						} else {
							atomic.AddInt64(&progress.SkipObjects, 1)
							atomic.AddInt64(&progress.SkipSize, objSize)
							logger.Infof("Worker %d: skipped copying object (already exists): %s", workerID, key)
						}
						continue
					}
				} else {
					logger.Infof("Worker %d: different origins, will upload directly for %s", workerID, key)
				}

				// 选择上传策略
				if objSize > c.copyOpt.PartSize {
					logger.Infof("Worker %d: object size %d exceeds part size %d, using multipart upload for %s",
						workerID, objSize, c.copyOpt.PartSize, key)
					// 对于大文件使用分块上传
					skip, uploadErr = s3cli.UploadMultipart(ctx, srcFS, f.Key, key, objMeta)
					if uploadErr != nil {
						atomic.AddInt64(&progress.FailObjects, 1)
						logger.Errorf("Worker %d: failed to multipart upload %s: %v", workerID, key, uploadErr)
					} else {
						if !skip {
							atomic.AddInt64(&progress.UploadObjects, 1)
							logger.Infof("Worker %d: successfully completed multipart upload: %s", workerID, key)
						} else {
							atomic.AddInt64(&progress.SkipObjects, 1)
							logger.Infof("Worker %d: skipped multipart upload (already exists): %s", workerID, key)
						}
					}
				} else {
					logger.Infof("Worker %d: object size %d is within part size %d, using simple upload for %s",
						workerID, objSize, c.copyOpt.PartSize, key)
					// 简单上传
					skip, uploadErr = s3cli.UploadObject(ctx, srcFS, f.Key, key, objMeta)
					if uploadErr != nil {
						atomic.AddInt64(&progress.FailObjects, 1)
						logger.Errorf("Worker %d: failed to upload %s: %v", workerID, key, uploadErr)
					} else {
						if !skip {
							atomic.AddInt64(&progress.UploadObjects, 1)
							atomic.AddInt64(&progress.UploadSize, objSize)
							logger.Infof("Worker %d: successfully uploaded: %s", workerID, key)
						} else {
							atomic.AddInt64(&progress.SkipObjects, 1)
							atomic.AddInt64(&progress.SkipSize, objSize)
							logger.Infof("Worker %d: skipped upload (already exists): %s", workerID, key)
						}
					}
				}
				// 处理上传错误
				if uploadErr != nil {
					logger.Debugf("Worker %d: handling error for upload: %s", workerID, key)
					if ok, _ := utils.HandleSpecialErrors(uploadErr, fmt.Sprintf("upload object %s", key)); ok {
						// 配额 或者权限 问题，就终止整个复制 任务，没有必要再重试下去
						logger.Errorf("Worker %d: critical error during upload, cancelling all tasks: %v", workerID, uploadErr)
						cancel()
					}
				}
			} // end for taskCh
		}(i) // end goroutine
	} // end worker loop

	// --- 主协程：发送任务 ---
	go func() {
		defer close(taskCh)
		for f := range fileCh {
			select {
			case <-ctx.Done():
				logger.Infof("[main sender] context cancelled, stopping sending tasks.")
				return
			case taskCh <- f:
			}
		}
		logger.Infof("[main sender] finished sending all tasks from fileCh.")
	}()

	// 等待所有工作goroutine完成
	wg.Wait()

	// 记录总执行时间和统计信息
	duration := time.Since(startTime)
	progress := utils.GetProgress()

	logger.Infof("Copy operation completed in %v", duration)
	logger.Infof("Statistics:")
	logger.Infof("  - Successfully uploaded: %d files (%d)", progress.UploadObjects, progress.UploadSize)
	logger.Infof("  - Skipped (already exists): %d files (%d)", progress.SkipObjects, progress.SkipSize)
	logger.Infof("  - Failed: %d files", progress.FailObjects)
	logger.Infof("  - Total processed: %d files", progress.UploadObjects+progress.SkipObjects+progress.FailObjects)

	if progress.FailObjects > 0 {
		logger.Warningf("Some files failed to copy. Check previous error logs for details.")
	}

	return nil
}
