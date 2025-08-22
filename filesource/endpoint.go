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
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
)

type EndpointConfig struct {
	Endpoint     string
	Bucket       string
	Prefix       string
	AccessKey    string
	SecretKey    string
	Region       string
	UsePathStyle bool
	UseSSL       bool
}

// ParseEndpoint parseEndpoint parses endpoint URL and extracts bucket information
// Supports formats:
// - http://bucket.endpoint.com (virtual hosted-style)
// - http://endpoint.com/bucket (path-style)
// - http://endpoint.com/bucket/prefix (path-style with prefix)
func ParseEndpoint(endpoint string, isDest bool) (*EndpointConfig, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint cannot be empty")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		logger.Errorf("failed to parse endpoint: %v", err)
		return nil, fmt.Errorf("invalid endpoint URL: %w", err)
	}

	config := &EndpointConfig{
		Region: "us-east-1",         // default region
		UseSSL: u.Scheme == "https", // 根据URL协议设置UseSSL
	}

	// Get credentials from environment variables
	if isDest {
		config.AccessKey = os.Getenv("DST_ACCESS_KEY")
		config.SecretKey = os.Getenv("DST_SECRET_KEY")
		if region := os.Getenv("DST_S3_REGION"); region != "" {
			config.Region = region
		}
	} else {
		config.AccessKey = os.Getenv("SRC_ACCESS_KEY")
		config.SecretKey = os.Getenv("SRC_SECRET_KEY")
		if region := os.Getenv("SRC_S3_REGION"); region != "" {
			config.Region = region
		}
	}

	if config.AccessKey == "" || config.SecretKey == "" {
		logger.Errorf("missing ak/sk in environment variables")
		return nil, fmt.Errorf("missing ak/sk in environment variables")
	}

	// Extract host without port for IP check
	hostWithoutPort := u.Hostname()
	if net.ParseIP(hostWithoutPort) != nil {
		// IP address format - always use path-style
		return parsePathStyle(u, config)
	}

	// 检查路径是否为空或只有斜杠
	path := strings.Trim(u.Path, "/")
	if path == "" {
		// 路径为空，尝试虚拟托管样式
		return parseVirtualHostStyle(u, config)
	}

	// 检查是否是已知云服务商的虚拟托管样式
	if isKnownVirtualHostStyle(u.Host) {
		return parseVirtualHostStyle(u, config)
	}

	// Fall back to path-style parsing
	return parsePathStyle(u, config)
}

// isVirtualHostStyle 检查是否是已知云服务商的虚拟托管样式
func isKnownVirtualHostStyle(host string) bool {
	// 移除端口部分（如果有）
	hostname := strings.Split(host, ":")[0]

	// AWS S3 虚拟托管样式模式
	// bucket.s3.region.amazonaws.com 或 bucket.s3.amazonaws.com
	awsPattern := regexp.MustCompile(`^[^.]+\.s3(?:[.-][^.]+)?\.amazonaws\.com$`)
	if awsPattern.MatchString(hostname) {
		return true
	}

	// 阿里云 OSS 虚拟托管样式模式
	// bucket.oss-region.aliyuncs.com
	aliyunPattern := regexp.MustCompile(`^[^.]+\.oss-[^.]+\.aliyuncs\.com$`)
	if aliyunPattern.MatchString(hostname) {
		return true
	}

	// 腾讯云 COS 虚拟托管样式模式
	// bucket.cos.region.myqcloud.com
	tencentPattern := regexp.MustCompile(`^[^.]+\.cos\.[^.]+\.myqcloud\.com$`)
	if tencentPattern.MatchString(hostname) {
		return true
	}

	// 七牛云 Kodo 虚拟托管样式模式
	// bucket.s3-region.qiniucs.com
	qiniuPattern := regexp.MustCompile(`^[^.]+\.s3-[^.]+\.qiniucs\.com$`)
	if qiniuPattern.MatchString(hostname) {
		return true
	}

	return false
}

// parsePathStyle handles path-style URLs (bucket in path)
func parsePathStyle(u *url.URL, config *EndpointConfig) (*EndpointConfig, error) {
	config.Endpoint = fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	config.UsePathStyle = true

	// Trim leading/trailing slashes and split path
	trimmedPath := strings.Trim(u.Path, "/")
	if trimmedPath == "" {
		logger.Errorf("invalid path style")
		return nil, fmt.Errorf("bucket not found in path for endpoint: %s", u.String())
	}

	// Split into bucket and optional prefix
	parts := strings.SplitN(trimmedPath, "/", 2)
	config.Bucket = parts[0]
	if len(parts) > 1 {
		config.Prefix = parts[1]
	}
	return config, nil
}

// parseVirtualHostStyle 处理虚拟托管样式URL
func parseVirtualHostStyle(u *url.URL, config *EndpointConfig) (*EndpointConfig, error) {
	// 使用第一个点分割主机名
	parts := strings.SplitN(u.Host, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid virtual host style URL: %s", u.String())
	}

	config.Bucket = parts[0]
	config.Endpoint = fmt.Sprintf("%s://%s", u.Scheme, parts[1])
	config.Prefix = strings.TrimPrefix(u.Path, "/")
	config.UsePathStyle = false
	return config, nil
}
