package writers

import (
	"io"
	"time"
)

// 写入时保存在write内部 调用Flush时再写入到writer里
type SpreadWriter struct {
	w        io.Writer
	interval time.Duration
	buf      [][]byte
	exitCh   chan int
}

func NewSpreadWriter(w io.Writer, interval time.Duration, exitCh chan int) *SpreadWriter {
	return &SpreadWriter{
		w:        w,
		interval: interval,
		buf:      make([][]byte, 0),
		exitCh:   exitCh,
	}
}

func (s *SpreadWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	s.buf = append(s.buf, b)
	return len(p), nil
}

func (s *SpreadWriter) Flush() {
	// 平分interval时间间隔 每sleep时间段发送一个数据 知道发送完毕
	sleep := s.interval / time.Duration(len(s.buf))
	ticker := time.NewTicker(sleep)
	for _, b := range s.buf {
		s.w.Write(b)
		select {
		case <-ticker.C:
		case <-s.exitCh: // skip sleeps finish writes
		}
	}
	ticker.Stop()
	s.buf = s.buf[:0]
}
