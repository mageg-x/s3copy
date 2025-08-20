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
	"time"
)

// ObjectInfo 包含对象的元数据信息
type ObjectInfo struct {
	Key          string            // 对象键（路径）
	Size         int64             // 对象大小（字节）
	LastModified time.Time         // 最后修改时间
	ETag         string            // 对象的ETag（用于校验）
	IsDir        bool              // 是否为目录
	Metadata     map[string]string // 其他元数据
}

// Source 定义了从各种来源读取数据的接口
type Source interface {
	Type() string

	// List 返回源中的对象列表
	// path: 要列出的路径
	// recursive: 是否递归列出子目录
	List(ctx context.Context, recursive bool) (<-chan ObjectInfo, <-chan error)

	// Read 读取指定对象的数据
	// path: 要读取的对象路径
	// offset: 从对象的哪个位置开始读取
	// 返回值:
	//   io.ReadCloser: 数据读取器
	//   int64: 对象的总大小
	//   error: 可能的错误
	Read(ctx context.Context, path string, offset int64) (io.ReadCloser, int64, error)

	// ReadRange 读取指定范围内的数据
	// path: 要读取的对象路径
	// start: 起始位置（包含）
	// end: 结束位置（包含）
	// 返回值:
	//   io.ReadCloser: 数据读取器
	//   error: 可能的错误
	ReadRange(ctx context.Context, path string, start, end int64) (io.ReadCloser, error)

	// GetMetadata 获取对象的元数据
	// path: 要获取元数据的对象路径
	// 返回值:
	//   map[string]string: 元数据键值对
	//   error: 可能的错误
	GetMetadata(ctx context.Context, path string) (map[string]string, error)
}

// contextAwareReader 在读取时检查上下文取消
type contextAwareReader struct {
	ctx context.Context
	io.ReadCloser
}

var logger = utils.GetLogger("s3copy")

func (r *contextAwareReader) Read(p []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.ReadCloser.Read(p)
}

func NewSource(srctype string, source string) (Source, error) {
	switch srctype {
	case "s3":
		return NewS3Source(source)
	case "file":
		return NewFileSource(source)
	case "http":
		return NewURLSource(source)
	default:
		logger.Errorf("unsupported source type: %s", srctype)
		return nil, fmt.Errorf("Unsupported source type: %s", srctype)
	}
}
