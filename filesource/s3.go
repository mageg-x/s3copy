package filesource

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Source 实现了从 S3 存储读取数据的 Source 接口
type S3Source struct {
	config   *EndpointConfig
	s3Client *s3.S3
}

// NewS3Source 创建一个新的 S3Source 实例
func NewS3Source(source string) (*S3Source, error) {
	config, err := ParseEndpoint(source, false)
	if err != nil {
		log.Fatalf("Failed to parse source endpoint: %v", err)
	}

	// 创建 AWS 会话
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		DisableSSL:       aws.Bool(!strings.HasPrefix(config.Endpoint, "https")),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	// 创建 S3 客户端
	s3Client := s3.New(sess)

	return &S3Source{
		config:   config,
		s3Client: s3Client,
	}, nil
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

		err := s.s3Client.ListObjectsV2PagesWithContext(ctx, params,
			func(page *s3.ListObjectsV2Output, lastPage bool) bool {
				// 处理对象
				for _, item := range page.Contents {
					info := ObjectInfo{
						Key:          *item.Key,
						Size:         *item.Size,
						LastModified: *item.LastModified,
						ETag:         strings.Trim(*item.ETag, `"`),
						Metadata:     make(map[string]string),
					}

					// 添加存储类别
					if item.StorageClass != nil {
						info.Metadata["storage_class"] = *item.StorageClass
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
						return false
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
							return false
						}
					}
				}

				return !lastPage
			})

		if err != nil {
			select {
			case errCh <- err:
			default:
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
	headResp, err := s.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: params.Bucket,
		Key:    params.Key,
	})
	if err != nil {
		return nil, 0, err
	}
	fullSize := *headResp.ContentLength

	// 设置 Range 头以支持偏移
	if offset > 0 {
		rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, fullSize-1)
		params.Range = aws.String(rangeHeader)
	}

	// 获取对象（流式）
	resp, err := s.s3Client.GetObjectWithContext(ctx, params)
	if err != nil {
		return nil, 0, err
	}

	// 创建上下文感知的读取器
	ctxReader := &contextAwareReader{
		ctx:        ctx,
		ReadCloser: resp.Body,
	}

	return ctxReader, fullSize, nil
}

// GetMetadata 获取 S3 对象的元数据
func (s *S3Source) GetMetadata(ctx context.Context, path string) (map[string]string, error) {
	// 构建获取对象元数据的请求
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path),
	}

	// 获取元数据
	resp, err := s.s3Client.HeadObjectWithContext(ctx, params)
	if err != nil {
		return nil, err
	}

	// 提取元数据
	metadata := make(map[string]string)
	metadata["is_dir"] = "false"

	// 内容长度
	metadata["size"] = strconv.FormatInt(*resp.ContentLength, 10)

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

	// 用户元数据
	for key, value := range resp.Metadata {
		metadata[key] = *value
	}

	return metadata, nil
}
