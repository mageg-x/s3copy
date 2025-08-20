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
	"net/url"
	"strings"
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

// normalizeMetadata 将元数据 key 强制转为小写，确保符合 AWS S3 标准
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
