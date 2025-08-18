package filesource

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// URLSource 实现了从 HTTP/HTTPS URL 读取数据的 Source 接口
type URLSource struct {
	httpClient *http.Client
	path       string // 包含URL列表的文件路径
}

// NewURLSource 创建一个新的 URLSource 实例
func NewURLSource(source string) (*URLSource, error) {
	return &URLSource{httpClient: &http.Client{}, path: source}, nil
}

// List 返回 URL 文件中的对象列表
func (s *URLSource) List(ctx context.Context, recursive bool) (<-chan ObjectInfo, <-chan error) {
	objChan := make(chan ObjectInfo)
	errChan := make(chan error, 1) // 缓冲通道，只需一个错误

	go func() {
		defer close(objChan)
		defer close(errChan)

		// 打开URL文件
		file, err := os.Open(s.path)
		if err != nil {
			errChan <- fmt.Errorf("failed to open URL file: %w", err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		lineNum := 0

		for scanner.Scan() {
			lineNum++
			url := strings.TrimSpace(scanner.Text())

			// 跳过空行和注释行
			if url == "" || strings.HasPrefix(url, "#") {
				continue
			}

			// 检查上下文是否已取消
			if ctx.Err() != nil {
				errChan <- ctx.Err()
				return
			}

			objChan <- ObjectInfo{
				Key:          url,
				Size:         0,          // 不需要实际大小
				LastModified: time.Now(), // 使用当前时间
				ETag:         "",         // 留空
				IsDir:        false,
				Metadata:     nil,
			}
		}

		if err := scanner.Err(); err != nil {
			errChan <- fmt.Errorf("error reading URL file at line %d: %w", lineNum, err)
		}
	}()

	return objChan, errChan
}

// Read 读取 URL 中的数据
func (s *URLSource) Read(ctx context.Context, path string, offset int64) (io.ReadCloser, int64, error) {
	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
	if err != nil {
		return nil, 0, err
	}

	// 设置 Range 头以支持偏移
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	// 发送请求
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	// 检查响应状态码
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("HTTP request failed with status: %s", resp.Status)
	}

	// 获取文件大小
	var fileSize int64
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		fileSize, _ = strconv.ParseInt(contentLength, 10, 64)
	}

	// 如果是部分响应，从Content-Range获取完整大小
	if resp.StatusCode == http.StatusPartialContent {
		if contentRange := resp.Header.Get("Content-Range"); contentRange != "" {
			parts := strings.Split(contentRange, "/")
			if len(parts) == 2 {
				fileSize, _ = strconv.ParseInt(parts[1], 10, 64)
			}
		}
	}

	return resp.Body, fileSize, nil
}

// GetMetadata 获取 URL 的元数据
func (s *URLSource) GetMetadata(ctx context.Context, path string) (map[string]string, error) {
	// 创建 HEAD 请求
	req, err := http.NewRequestWithContext(ctx, "HEAD", path, nil)
	if err != nil {
		return nil, err
	}

	// 发送请求
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 检查响应状态码
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP HEAD request failed with status: %s", resp.Status)
	}

	// 提取元数据
	metadata := make(map[string]string)
	metadata["is_dir"] = "false"

	// 内容长度
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		metadata["size"] = contentLength
	}

	// 内容类型
	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		metadata["content_type"] = contentType
	}

	// ETag
	if etag := resp.Header.Get("ETag"); etag != "" {
		metadata["etag"] = etag
	}

	// 最后修改时间
	if lastModified := resp.Header.Get("Last-Modified"); lastModified != "" {
		metadata["last_modified"] = lastModified
	}

	return metadata, nil
}
