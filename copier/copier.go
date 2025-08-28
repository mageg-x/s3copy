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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// 打开上传
	s3cli, err := Create(c.destConfig, 3, int(c.copyOpt.PartSize), c.copyOpt.Concurrent)
	if err != nil {
		logger.Errorf("failed to create s3 client: %v", err)
		return err
	}
	// --- 使用 context 控制取消 ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在 Copy 函数结束时取消 context

	// 如果目标bucket不存在就创建
	if ok, err := s3cli.IsBucketExist(ctx, c.destConfig.Bucket); err != nil || !ok {
		_ = s3cli.CreateBucket(ctx, c.destConfig.Bucket)
	}

	if ok, err := s3cli.IsBucketExist(ctx, c.destConfig.Bucket); err != nil || !ok {
		logger.Errorf("dst bucket does not exist")
		return errors.New("dst bucket does not exist")
	}

	// 判断是否同源，可以使用copyObject
	isSameOrigin := false
	var srcEndPoint *source.EndpointConfig
	if c.copyOpt.SourceType == "s3" {
		ep, err := source.ParseEndpoint(c.copyOpt.SourcePath, false)
		srcEndPoint = ep
		if err == nil && srcEndPoint != nil {
			isSameOrigin = s3cli.CanUseCopyObject(ctx, srcEndPoint, c.destConfig)
		}
	}

	// 创建文件源
	srcFS, err := source.NewSource(c.copyOpt.SourceType, c.copyOpt.SourcePath)
	if err != nil {
		logger.Fatalf("failed to create source: %v", err)
	}

	// 为了统计需要，要知道文件总个数和总大小
	go c.Stat(srcFS)
	go utils.StartProgressReporter(context.Background(), utils.GetProgress())

	// 获取文件列表
	fileCh, errCh := srcFS.List(ctx, true)

	// --- 主协程：监听 errCh ---
	go func() {
		select {
		case err := <-errCh:
			if err != nil {
				logger.Errorf("遍历文件失败: %v", err)
				cancel()
			}
		case <-ctx.Done(): // 如果主 context 已结束，就不处理了
			return
		}
	}()

	// 控制并发数量
	concurrency := 10
	if c.copyOpt.Concurrent > 0 {
		concurrency = c.copyOpt.Concurrent
	}

	// 创建任务通道
	taskCh := make(chan source.ObjectInfo, 100)
	var wg sync.WaitGroup

	// 消费者goroutine
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range taskCh {
				// 检查 context 是否已取消（由其他 worker 或主协程触发）
				select {
				case <-ctx.Done():
					logger.Infof("[worker] context cancelled, stopping worker.")
					return // 退出当前 worker
				default:
				}
				logger.Debugf("prepare upload file info is: %+v", f)
				if f.IsDir {
					continue
				}
				// 1. 获取对象元数据
				objMeta, err := srcFS.GetMetadata(ctx, f.Key)
				if err != nil {
					logger.Errorf("failed to get metadata for %s: %v", f.Key, err)
					continue
				}
				logger.Debugf("get copy option %v object meta %v", c.copyOpt, objMeta)

				// 2. 确定对象大小
				var objSize int64
				sizeStr, ok := objMeta["size"]
				if ok {
					objSize, _ = strconv.ParseInt(sizeStr, 10, 64)
				}

				key := f.Key
				switch c.copyOpt.SourceType {
				case "file":
					// 假设 sourcePath 是类似 "/data/input/" 或 "/data/input" 的路径
					prefix := filepath.Dir(c.copyOpt.SourcePath)
					// 去掉前缀
					key = strings.TrimPrefix(key, prefix)
					// 去掉开头和结尾的 '/'，避免出现 "/project/file.txt" 或 "project/file.txt/"
					key = strings.Trim(key, "/")
				case "http":
					key, err = utils.GetFileNameFromURL(c.copyOpt.SourcePath)
					if err != nil {
						logger.Errorf("failed to get file name from url: %v", err)
						continue
					}
				}

				logger.Debugf("get from key %s to key is: %s", f.Key, key)
				progress := utils.GetProgress()
				var uploadErr error
				var skip bool
				if isSameOrigin {
					skip, uploadErr = s3cli.CopyObject(ctx, srcEndPoint, f.Key, key, objMeta)
					if uploadErr != nil {
						atomic.AddInt64(&progress.FailObjects, 1)
						logger.Errorf("failed to copy object %s, %v", key, uploadErr)
					} else {
						if !skip {
							atomic.AddInt64(&progress.UploadObjects, 1)
							atomic.AddInt64(&progress.UploadSize, objSize)
						} else {
							atomic.AddInt64(&progress.SkipObjects, 1)
							atomic.AddInt64(&progress.SkipSize, objSize)
						}
						continue
					}
				}

				if objSize > c.copyOpt.PartSize {
					// 对于大文件使用分块上传
					skip, uploadErr = s3cli.UploadMultipart(ctx, srcFS, f.Key, key, objMeta)
					if uploadErr != nil {
						atomic.AddInt64(&progress.FailObjects, 1)
						logger.Errorf("failed to multiupload %s, %v", key, uploadErr)
					} else {
						if !skip {
							atomic.AddInt64(&progress.UploadObjects, 1)
						} else {
							atomic.AddInt64(&progress.SkipObjects, 1)
						}
					}
				} else {
					// 简单上传
					skip, uploadErr = s3cli.UploadObject(ctx, srcFS, f.Key, key, objMeta)
					if uploadErr != nil {
						atomic.AddInt64(&progress.FailObjects, 1)
						logger.Errorf("failed to upload %s, %v", key, uploadErr)
					} else {
						if !skip {
							atomic.AddInt64(&progress.UploadObjects, 1)
							atomic.AddInt64(&progress.UploadSize, objSize)
						} else {
							atomic.AddInt64(&progress.SkipObjects, 1)
							atomic.AddInt64(&progress.SkipSize, objSize)
						}
					}
				}
				if uploadErr != nil {
					if ok, _ := utils.HandleSpecialErrors(uploadErr, fmt.Sprintf("upload object %s", key)); ok {
						// 配额 或者权限 问题，就终止整个复制 任务，没有必要再重试下去
						cancel()
					}
				}
			} // end for taskCh
		}() // end goroutine
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

	return nil
}
