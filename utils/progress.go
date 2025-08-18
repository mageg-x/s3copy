package utils

import (
	"context"
	"fmt"
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

		line := fmt.Sprintf(`{"total_size":%d,"migrated_size":%d,"migrated_objects":%d,"average_speed":%d,"progress":%d}`,
			total, upload, upload_objects, avg_speed, percent)
		if result != "" {
			line = fmt.Sprintf(`{"total_size":%d,"migrated_size":%d,"migrated_objects":%d,"average_speed":%d,"progress":%d, "extra_info":%s}`,
				total, upload, upload_objects, avg_speed, percent, result)
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
