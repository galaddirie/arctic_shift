import sys
import os
import time
from typing import BinaryIO


class FileProgressLog:
	file: BinaryIO
	fileSize: int
	i: int
	startTime: float
	printEvery: int
	maxLineLength: int

	def __init__(self, path: str, file: BinaryIO):
		self.file = file
		self.fileSize = os.path.getsize(path)
		self.i = 0
		self.startTime = time.time()
		self.printEvery = 10_000
		self.maxLineLength = 0
	
	def onRow(self):
		self.i += 1
		if self.i % self.printEvery == 0 and self.i > 0:
			self.logProgress()
		
	def logProgress(self, end=""):
		progress = self.file.tell() / self.fileSize if not self.file.closed else 1
		elapsed = time.time() - self.startTime
		remaining = (elapsed / progress - elapsed) if progress > 0 else 0
		timePerRow = elapsed / self.i
		printStr = f"{self.i:,} - {progress:.2%} - elapsed: {formatTime(elapsed)} - remaining: {formatTime(remaining)} - {formatTime(timePerRow)}/row"
		self.maxLineLength = max(self.maxLineLength, len(printStr))
		printStr = printStr.ljust(self.maxLineLength)
		print(f"\r{printStr}", end=end)

		if timePerRow < 20/1000/1000:
			self.printEvery = 20_000
		elif timePerRow < 50/1000/1000:
			self.printEvery = 10_000
		else:
			self.printEvery = 5_000

def formatTime(seconds: float) -> str:
	if seconds == 0:
		return "0s"
	if seconds < 0.001:
		return f"{seconds * 1_000_000:.1f}µs"
	if seconds < 1:
		return f"{seconds * 1_000:.2f}ms"
	elapsedHr = int(seconds // 3600)
	elapsedMin = int((seconds % 3600) // 60)
	elapsedSec = int(seconds % 60)
	return f"{elapsedHr:02}:{elapsedMin:02}:{elapsedSec:02}"


import psutil
from datetime import timedelta
class ProgressLogger:
    def __init__(self, total_rows, update_interval=1.0):
        self.total_rows = total_rows
        self.processed_rows = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.update_interval = update_interval
        self.process = psutil.Process()

    def update(self, rows_processed):
        self.processed_rows += rows_processed
        current_time = time.time()

        if current_time - self.last_update_time >= self.update_interval:
            self._print_progress()
            self.last_update_time = current_time

    def _print_progress(self):
        elapsed_time = time.time() - self.start_time
        progress = self.processed_rows / self.total_rows
        remaining_time = (elapsed_time / progress) - elapsed_time if progress > 0 else 0

        cpu_percent = self.process.cpu_percent()
        memory_usage = self.process.memory_info().rss / 1024 / 1024  # Convert to MB

        status = f"\r{self.processed_rows:,} - {progress:.2%} - "
        status += f"elapsed: {timedelta(seconds=int(elapsed_time))} - "
        status += f"remaining: {timedelta(seconds=int(remaining_time))} - "
        status += f"{(elapsed_time / self.processed_rows) * 1e6:.1f}µs/row - "
        status += f"CPU: {cpu_percent:.1f}% - RAM: {memory_usage:.1f}MB"

        sys.stdout.write(status)
        sys.stdout.flush()

    def finish(self):
        self._print_progress()
        print("\nProcessing completed.")