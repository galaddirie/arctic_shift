package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

type FileProgressLog struct {
	file           *os.File
	fileSize       int64
	i              int64
	startTime      time.Time
	maxLineLength  int
	lastUpdate     time.Time
	updateInterval time.Duration
}

func NewFileProgressLog(path string, file *os.File) (*FileProgressLog, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %v", err)
	}

	return &FileProgressLog{
		file:           file,
		fileSize:       fileInfo.Size(),
		i:              0,
		startTime:      time.Now(),
		maxLineLength:  0,
		lastUpdate:     time.Now(),
		updateInterval: 100 * time.Millisecond,
	}, nil
}

func (fpl *FileProgressLog) OnRow() {
	fpl.i++
	if time.Since(fpl.lastUpdate) >= fpl.updateInterval {
		fpl.LogProgress("")
		fpl.lastUpdate = time.Now()
	}
}

func (fpl *FileProgressLog) LogProgress(end string) {
	currentPosition, err := fpl.file.Seek(0, io.SeekCurrent)
	if err != nil {
		fmt.Printf("Error getting current file position: %v\n", err)
		return
	}
	progress := float64(currentPosition) / float64(fpl.fileSize)
	elapsed := time.Since(fpl.startTime)
	var remaining time.Duration
	if progress > 0 {
		remaining = time.Duration(float64(elapsed)/progress) - elapsed
	}
	timePerRow := elapsed / time.Duration(fpl.i)

	printStr := fmt.Sprintf("%d - %.2f%% - elapsed: %s - remaining: %s - %s/row",
		fpl.i, progress*100, formatTime(elapsed), formatTime(remaining), formatTime(timePerRow))

	if len(printStr) > fpl.maxLineLength {
		fpl.maxLineLength = len(printStr)
	}
	printStr = fmt.Sprintf("\r%-*s", fpl.maxLineLength, printStr)
	fmt.Print(printStr + end)
}

func formatTime(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Microsecond {
		return fmt.Sprintf("%.1fns", float64(d.Nanoseconds()))
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Milliseconds()))
	}
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}