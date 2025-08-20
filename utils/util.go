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

package utils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/smithy-go"
	"net/url"
	"strings"
	"time"
)

var logger = GetLogger("s3copy")

func GetFileNameFromURL(urlStr string) (string, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		logger.Errorf("error parsing URL: %v", err)
		return "", fmt.Errorf("invalid URL: %v", err)
	}

	path := parsedURL.Path
	if path == "" || path == "/" {
		return "downloaded_file", nil
	}

	segments := strings.Split(path, "/")
	filename := segments[len(segments)-1]

	if filename == "" {
		return "downloaded_file", nil
	}

	return filename, nil
}

func Unique[T comparable](slice []T) []T {
	seen := make(map[T]struct{})
	var result []T
	for _, v := range slice {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// NormalizeMetadata normalizeMetadata 将元数据 key 强制转为小写，确保符合 AWS S3 标准
func NormalizeMetadata(metadata map[string]*string) map[string]*string {
	if metadata == nil {
		return nil
	}
	normalized := make(map[string]*string, len(metadata))
	for k, v := range metadata {
		lowerKey := strings.ToLower(k)
		normalized[lowerKey] = v
	}
	return normalized
}

// WithRetry withRetry 是一个通用的重试包装函数，处理特殊错误和重试逻辑
func WithRetry(operation string, maxRetries int, fn func() error) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		// 处理特殊错误
		if handled, specialErr := HandleSpecialErrors(err, operation); handled {
			return specialErr
		}

		logger.Errorf("Operation %s failed (attempt %d/%d): %v", operation, attempt+1, maxRetries, err)
		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("operation %s failed after %d attempts", operation, maxRetries)
}

// HandleSpecialErrors handleSpecialErrors 处理特殊错误类型
func HandleSpecialErrors(err error, operation string) (bool, error) {
	if err == nil {
		return false, nil
	}

	// 定义特殊错误代码
	const (
		ErrCodeAccessDenied  = "AccessDenied"
		ErrCodeQuotaExceeded = "QuotaExceeded"
	)

	// 错误信息映射
	errorMessages := map[string]string{
		ErrCodeAccessDenied:  "Check IAM permissions and bucket policies.",
		ErrCodeQuotaExceeded: "Try again later or request quota increase.",
	}

	// 检查错误类型
	var errCode string

	// 处理 AWS 错误
	if aerr, ok := err.(awserr.Error); ok {
		errCode = aerr.Code()
	} else if apiErr, ok := err.(smithy.APIError); ok {
		errCode = apiErr.ErrorCode()
	} else if strings.Contains(err.Error(), ErrCodeAccessDenied) {
		errCode = ErrCodeAccessDenied
	} else if strings.Contains(err.Error(), ErrCodeQuotaExceeded) {
		errCode = ErrCodeQuotaExceeded
	}

	// 处理特殊错误
	if message, ok := errorMessages[errCode]; ok {
		logger.Errorf("%s during %s: %v. %s", errCode, operation, err, message)
		return true, fmt.Errorf("%s: %w", errCode, err)
	}

	return false, nil
}
