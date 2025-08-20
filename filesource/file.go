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
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/afero"
)

// FileSource 实现了从本地文件系统读取数据的 Source 接口
type FileSource struct {
	fs   afero.Fs
	path string
}

// NewFileSource 创建一个新的 FileSource 实例
func NewFileSource(source string) (*FileSource, error) {
	// 如果文件不存在，返回错误
	if _, err := os.Lstat(source); err != nil {
		return nil, fmt.Errorf("file source %q already exists", source)
	}

	return &FileSource{fs: afero.NewOsFs(), path: source}, nil
}

func (s *FileSource) Type() string {
	return "file"
}

func (s *FileSource) List(ctx context.Context, recursive bool) (<-chan ObjectInfo, <-chan error) {
	objectCh := make(chan ObjectInfo, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(objectCh)
		defer close(errCh)

		// 检查路径是否存在
		info, err := os.Lstat(s.path)
		if err != nil {
			logger.Errorf("failed to stat path %s: %v", s.path, err)
			errCh <- err
			return
		}
		logger.Infof("lstat %s mode: %v, isSymlink: %v", s.path, info.Mode(), info.Mode()&os.ModeSymlink != 0)
		if info.Mode()&os.ModeSymlink != 0 {
			logger.Infof("lstat %s is %v", s.path, info)
			return
		}

		// 如果是单个文件
		if !info.IsDir() {
			select {
			case objectCh <- ObjectInfo{
				Key:          s.path,
				Size:         info.Size(),
				LastModified: info.ModTime(),
				IsDir:        false,
			}:
			case <-ctx.Done():
				errCh <- ctx.Err()
			}
			return
		}

		// 遍历目录
		walkFn := func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				logger.Errorf("failed to open file %s: %v", filePath, err)
				return err
			}

			// 跳过根目录
			if filePath == s.path {
				return nil
			}
			info, err = os.Lstat(filePath)
			if err != nil || (info.Mode()&os.ModeSymlink != 0) {
				return nil
			}
			// 非递归模式跳过子目录
			if !recursive && info.IsDir() {
				return filepath.SkipDir
			}

			// 创建对象信息
			objInfo := ObjectInfo{
				Key:          filePath,
				Size:         info.Size(),
				LastModified: info.ModTime(),
				IsDir:        info.IsDir(),
			}

			select {
			case objectCh <- objInfo:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}

		if err := afero.Walk(s.fs, s.path, walkFn); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	return objectCh, errCh
}

// Read 读取本地文件的数据（流式）
func (s *FileSource) Read(ctx context.Context, path string, offset int64) (io.ReadCloser, int64, error) {
	// 打开文件
	file, err := s.fs.Open(path)
	if err != nil {
		return nil, 0, err
	}

	// 获取文件信息
	info, err := file.Stat()
	if err != nil {
		file.Close()
		logger.Errorf("failed to stat file %s: %v", path, err)
		return nil, 0, err
	}

	// 检查文件大小
	fileSize := info.Size()

	// 设置偏移量
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			logger.Errorf("failed to seek file %s: %v", path, err)
			return nil, 0, err
		}
	}

	// 创建上下文感知的读取器
	ctxReader := &contextAwareReader{
		ctx:        ctx,
		ReadCloser: file,
	}

	return ctxReader, fileSize, nil
}

// ReadRange 读取指定范围内的本地文件数据
func (s *FileSource) ReadRange(ctx context.Context, path string, start, end int64) (io.ReadCloser, error) {
	// 打开文件
	file, err := s.fs.Open(path)
	if err != nil {
		return nil, err
	}

	// 获取文件信息
	info, err := file.Stat()
	if err != nil {
		file.Close()
		logger.Errorf("failed to stat file %s: %v", path, err)
		return nil, err
	}

	// 检查文件大小
	fileSize := info.Size()

	// 验证范围
	if start < 0 || end >= fileSize || start > end {
		file.Close()
		return nil, fmt.Errorf("invalid range: start=%d, end=%d, fileSize=%d", start, end, fileSize)
	}

	// 设置偏移量
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		file.Close()
		logger.Errorf("failed to seek file %s: %v", path, err)
		return nil, err
	}

	// 创建有限制的读取器
	limitedReader := io.LimitReader(file, end-start+1)

	// 创建上下文感知的读取器
	ctxReader := &contextAwareReader{
		ctx:        ctx,
		ReadCloser: io.NopCloser(limitedReader),
	}

	return ctxReader, nil
}

// GetMetadata 获取文件的元数据
func (s *FileSource) GetMetadata(_ context.Context, path string) (map[string]string, error) {
	info, err := s.fs.Stat(path)
	if err != nil {
		logger.Errorf("failed to stat file %s: %v", path, err)
		return nil, err
	}

	metadata := map[string]string{
		"size":          strconv.FormatInt(info.Size(), 10),
		"last_modified": info.ModTime().Format(time.RFC3339),
		"is_dir":        strconv.FormatBool(info.IsDir()),
	}

	return metadata, nil
}
