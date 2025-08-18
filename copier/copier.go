package copier

import (
	"context"
	"fmt"
	"log"
	source "s3copy/filesource"
	"s3copy/utils"
	"strconv"
	"strings"
)

// Copier 处理从文件源到S3的复制
type Copier struct {
	copyOpt    *CopyOptions
	destConfig *source.EndpointConfig
}

// CopyOptions 包含复制操作的选项
type CopyOptions struct {
	SourcePath string // 源路径
	SourceType string // 源类型: file, url, s3
	DestPath   string // 目标路径
	DestType   string // 目标类型: 固定 s3
	Concurrent int    // 并发上传数量
	PartSize   int64  // 传输分块大小
}

// NewCopier 创建文件复制器实例
func NewCopier(opt *CopyOptions) (*Copier, error) {
	if opt == nil {
		log.Fatalf("copy option is required")
	}
	destConfig, err := source.ParseEndpoint(opt.DestPath, true)
	if err != nil {
		log.Fatalf("failed to parse destination endpoint: %v", err)
	}
	return &Copier{
		copyOpt:    opt,
		destConfig: destConfig,
	}, nil
}

func (c *Copier) Copy() error {
	// 打开上传
	s3cli, err := utils.Create(c.destConfig, 3, c.copyOpt.PartSize, c.copyOpt.Concurrent)
	if err != nil {
		log.Printf("Failed to create S3 client: %v", err)
		return err
	}

	// 创建文件源
	srcFS, err := source.NewSource(c.copyOpt.SourceType, c.copyOpt.SourcePath)
	if err != nil {
		log.Fatalf("Failed to create source: %v", err)
	}
	// 获取文件列表
	fileCh, errCh := srcFS.List(context.Background(), true)

	// 处理文件
	for f := range fileCh {
		log.Printf(": %+v", f)
		if f.IsDir {
			continue
		}
		// 1. 获取对象元数据
		objMeta, err := srcFS.GetMetadata(context.Background(), f.Key)
		if err != nil {
			return fmt.Errorf("failed to get metadata for %s: %w", f.Key, err)
		}
		log.Printf("get object meta %v", objMeta)

		// 2. 确定对象大小
		var objSize int64
		sizeStr, ok := objMeta["size"]
		if ok {
			objSize, _ = strconv.ParseInt(sizeStr, 10, 64)
		}
		// 3. 打开源对象
		reader, size, err := srcFS.Read(context.Background(), f.Key, 0)
		if err != nil {
			log.Printf("Failed to read %s: %v", f.Key, err)
			continue
		}
		defer reader.Close()
		log.Printf("read %s size %d|%d", f.Key, size, objSize)
		key := f.Key
		if c.copyOpt.SourceType == "file" {
			// 假设 sourcePath 是类似 "/data/input/" 或 "/data/input" 的路径
			prefix := c.copyOpt.SourcePath

			// 去掉前缀
			key = strings.TrimPrefix(key, prefix)

			// 去掉开头和结尾的 '/'，避免出现 "/project/file.txt" 或 "project/file.txt/"
			key = strings.Trim(key, "/")
		}
		if objSize > c.copyOpt.PartSize {
			// 对于大文件使用分块上传
		} else {
			// 简单上传
			err = s3cli.UploadObject(context.Background(), key, reader, objMeta)
			if err != nil {
				log.Printf("Failed to upload %s: %v", key, err)
			}
		}
	}

	// 检查错误
	if err := <-errCh; err != nil {
		log.Fatalf("遍历文件失败: %v", err)
	}
	return nil
}
