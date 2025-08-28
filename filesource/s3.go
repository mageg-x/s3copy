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

package filesource

import (
	"context"
	"fmt"
	"io"
	"s3copy/utils"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconf "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Source 实现了从 S3 存储读取数据的 Source 接口
type S3Source struct {
	config   *EndpointConfig
	s3Client *s3.Client
}

// NewS3Source 创建一个新的 S3Source 实例
func NewS3Source(source string) (*S3Source, error) {
	config, err := ParseEndpoint(source, false)
	if err != nil {
		logger.Fatalf("failed to parse source endpoint: %v", err)
	}

	// 创建 AWS 配置
	ctx := context.Background()

	// 创建凭证提供者
	credentialProvider := credentials.NewStaticCredentialsProvider(
		config.AccessKey,
		config.SecretKey,
		"",
	)

	// 加载默认配置
	cfg, err := awsconf.LoadDefaultConfig(ctx,
		awsconf.WithRegion(config.Region),
		awsconf.WithCredentialsProvider(credentialProvider),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load SDK configuration: %w", err)
	}

	// 创建 S3 客户端
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if config.Endpoint != "" {
			o.BaseEndpoint = aws.String(config.Endpoint)
		}
		o.UsePathStyle = config.UsePathStyle
		o.Logger = utils.NullLogger{}
	})

	// 测试数据源是否可达
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(config.Bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("source bucket not exist: %w", err)
	}

	return &S3Source{
		config:   config,
		s3Client: client,
	}, nil
}

func (s *S3Source) Type() string {
	return "s3"
}

func (s *S3Source) List(ctx context.Context, recursive bool) (<-chan ObjectInfo, <-chan error) {
	objectCh := make(chan ObjectInfo, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(objectCh)
		defer close(errCh)

		params := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.config.Bucket),
			Prefix: aws.String(s.config.Prefix),
		}

		if !recursive {
			params.Delimiter = aws.String("/")
		}

		paginator := s3.NewListObjectsV2Paginator(s.s3Client, params)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			// 处理对象
			for _, item := range page.Contents {
				info := ObjectInfo{
					Key:          *item.Key,
					Size:         aws.ToInt64(item.Size),
					LastModified: *item.LastModified,
					ETag:         strings.Trim(*item.ETag, `"`),
					Metadata:     make(map[string]string),
				}

				// 添加存储类别
				if item.StorageClass != "" {
					info.Metadata["storage_class"] = string(item.StorageClass)
				}

				// 添加所有者信息
				if item.Owner != nil {
					if item.Owner.DisplayName != nil {
						info.Metadata["owner_name"] = *item.Owner.DisplayName
					}
					if item.Owner.ID != nil {
						info.Metadata["owner_id"] = *item.Owner.ID
					}
				}

				select {
				case objectCh <- info:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			}

			// 处理目录（在非递归模式下）
			if !recursive {
				for _, prefix := range page.CommonPrefixes {
					info := ObjectInfo{
						Key:   *prefix.Prefix,
						IsDir: true,
					}

					select {
					case objectCh <- info:
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					}
				}
			}
		}
	}()

	return objectCh, errCh
}

// Read 流式读取 S3 对象的数据
func (s *S3Source) Read(ctx context.Context, path string, offset int64) (io.ReadCloser, int64, error) {
	// 构建获取对象的请求
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path),
	}

	// 获取完整对象大小
	headResp, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: params.Bucket,
		Key:    params.Key,
	})
	if err != nil {
		logger.Errorf("failed to head s3 object: %v", err)
		return nil, 0, err
	}
	fullSize := headResp.ContentLength

	// 设置 Range 头以支持偏移
	if offset > 0 {
		rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, aws.ToInt64(fullSize)-1)
		params.Range = aws.String(rangeHeader)
	}

	// 获取对象（流式）
	resp, err := s.s3Client.GetObject(ctx, params)
	if err != nil {
		logger.Errorf("failed to get s3 object: %v", err)
		return nil, 0, err
	}

	// 创建上下文感知的读取器
	ctxReader := &contextAwareReader{
		ctx:        ctx,
		ReadCloser: resp.Body,
	}

	return ctxReader, aws.ToInt64(fullSize), nil
}

// ReadRange 读取指定范围内的S3对象数据
func (s *S3Source) ReadRange(ctx context.Context, path string, start, end int64) (io.ReadCloser, error) {
	// 构建获取对象的请求
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path),
	}

	// 获取完整对象大小以验证范围
	headResp, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: params.Bucket,
		Key:    params.Key,
	})
	if err != nil {
		logger.Errorf("failed to head s3 object: %v", err)
		return nil, err
	}
	fullSize := headResp.ContentLength

	// 验证范围
	if start < 0 || end >= aws.ToInt64(fullSize) || start > end {
		return nil, fmt.Errorf("invalid range: start=%d, end=%d, fullSize=%d", start, end, fullSize)
	}

	// 设置 Range 头
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	params.Range = aws.String(rangeHeader)

	// 获取对象（流式）
	resp, err := s.s3Client.GetObject(ctx, params)
	if err != nil {
		logger.Errorf("failed to get s3 object: %v", err)
		return nil, err
	}

	// 创建上下文感知的读取器
	ctxReader := &contextAwareReader{
		ctx:        ctx,
		ReadCloser: resp.Body,
	}

	return ctxReader, nil
}

// GetMetadata 获取 S3 对象的元数据
func (s *S3Source) GetMetadata(ctx context.Context, path string) (map[string]string, error) {
	// 构建获取对象元数据的请求
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path),
	}

	// 获取元数据
	resp, err := s.s3Client.HeadObject(ctx, params)
	if err != nil {
		logger.Errorf("failed to head s3 object for metadata: %v", err)
		return nil, err
	}

	// 提取元数据
	metadata := make(map[string]string)

	// 用户元数据
	for key, value := range resp.Metadata {
		metadata[key] = value
	}

	metadata["is_dir"] = "false"

	// 内容长度
	metadata["size"] = strconv.FormatInt(aws.ToInt64(resp.ContentLength), 10)

	// 内容类型
	if resp.ContentType != nil {
		metadata["content_type"] = *resp.ContentType
	}

	// ETag
	if resp.ETag != nil {
		metadata["etag"] = strings.Trim(*resp.ETag, `"`)
	}

	// 最后修改时间
	if resp.LastModified != nil {
		metadata["last_modified"] = resp.LastModified.Format("2006-01-02T15:04:05Z07:00")
	}

	return metadata, nil
}
