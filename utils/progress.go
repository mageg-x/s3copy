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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Progress struct {
	TotalSize     int64
	TotalObjects  int64
	DownloadSize  int64
	UploadSize    int64
	UploadObjects int64
	StartTime     time.Time
}

var (
	prog *Progress
	once sync.Once
)

func GetProgress() *Progress {
	once.Do(func() {
		prog = &Progress{StartTime: time.Now()}
	})
	return prog
}

func (p *Progress) Report(result string) {
	now := time.Since(p.StartTime).Seconds()
	if now > 0 {
		total := atomic.LoadInt64(&p.TotalSize)
		upload := atomic.LoadInt64(&p.UploadSize)
		upload_objects := atomic.LoadInt64(&p.UploadObjects)
		total_objects := atomic.LoadInt64(&p.TotalObjects)
		avg_speed := int64(float64(upload) / now)
		progress1 := upload * 100 / max(total, 1)
		progress2 := upload_objects * 100 / max(total_objects, 1)
		percent := min(progress1, progress2)

		line := fmt.Sprintf(`{"total_size":%d,"migrated_size":%d,"total_objects":%d, migrated_objects":%d,"average_speed":%d,"progress":%d}`,
			total, upload, total_objects, upload_objects, avg_speed, percent)
		if result != "" {
			line = fmt.Sprintf(`{"total_size":%d,"migrated_size":%d,"total_objects":%d, "migrated_objects":%d,"average_speed":%d,"progress":%d, "extra_info":%s}`,
				total, upload, total_objects, upload_objects, avg_speed, percent, result)
		}
		// 打印line 到控制台
		fmt.Println(line)
	}
}

func StartProgressReporter(ctx context.Context, progress *Progress) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			progress.Report("")
		}
	}
}
