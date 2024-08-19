import sys
import csv
import os
import orjson
import re
from typing import Dict, List
import zstandard as zstd
import multiprocessing as mp
from queue import Empty
from datetime import datetime, UTC
import pickle
import mmap
import numpy as np
from collections import defaultdict
import io

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# Ensure Python 3.10+
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")

fileOrFolderPath = r"D:\reddit\dumps\reddit\submissions"
recursive = False
output_dir = r"D:\reddit\dumps\reddit\submissions\organized"
DEFAULT_WHITELIST_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'whitelist.csv')
BLACKLISTED_FILES = {}
CHUNK_SIZE = 100_000  # Number of rows to process before writing to disk
CHECKPOINT_FILE = "checkpoint.pkl"
BUFFER_SIZE = 1024 * 1024  # 1MB buffer for writing

NUM_PROCESSES = mp.cpu_count()  # Use all available CPU cores

def read_whitelist(csv_path):
    whitelist = set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            subreddit = row['name'].lower()
            if subreddit.startswith('r/'):
                subreddit = subreddit[2:]
            whitelist.add(subreddit)
    return whitelist

WHITELIST = read_whitelist(DEFAULT_WHITELIST_PATH)

def create_perfect_hash(whitelist):
    return {item: True for item in whitelist}

WHITELIST_HASH = create_perfect_hash(WHITELIST)

def sanitize_subreddit_name(name: str) -> str:
    sanitized = re.sub(r'[^\w\-]', '', name)
    return sanitized[:50]

def get_month_year_vectorized(created_utc_array):
    dates = np.array(created_utc_array, dtype='datetime64[s]')
    return np.datetime_as_string(dates, unit='M')

def process_batch(batch):
    subreddits = [row["subreddit"] for row in batch]
    created_utcs = [row["created_utc"] for row in batch]
    
    month_years = get_month_year_vectorized(created_utcs)
    
    result = defaultdict(lambda: defaultdict(list))
    for row, month_year, subreddit in zip(batch, month_years, subreddits):
        if WHITELIST_HASH.get(subreddit, False):
            result[month_year][subreddit].append(row)
    
    return result

class BufferedWriter:
    def __init__(self, directory):
        self.directory = directory
        self.buffers = defaultdict(io.BytesIO)
        self.file_handles = {}

    def write(self, month_year, subreddit, data):
        key = (month_year, subreddit)
        self.buffers[key].write(data)
        if self.buffers[key].tell() >= BUFFER_SIZE:
            self.flush(key)

    def flush(self, key=None):
        if key is None:
            keys = list(self.buffers.keys())
        else:
            keys = [key]
        
        for k in keys:
            month_year, subreddit = k
            if k not in self.file_handles:
                sanitized_subreddit = sanitize_subreddit_name(subreddit)
                month_dir = os.path.join(self.directory, month_year)
                os.makedirs(month_dir, exist_ok=True)
                file_path = os.path.join(month_dir, f"{sanitized_subreddit}.jsonl")
                self.file_handles[k] = open(file_path, 'ab')
            
            self.file_handles[k].write(self.buffers[k].getvalue())
            self.buffers[k] = io.BytesIO()

    def close(self):
        self.flush()
        for handle in self.file_handles.values():
            handle.close()




def process_file(path: str, writer: BufferedWriter, start_position: int = 0):
    print(f"Processing file {path} from position {start_position}")
    file_name = os.path.basename(path)
    if file_name in BLACKLISTED_FILES:
        print(f"Skipping blacklisted file: {file_name}")
        return start_position

    try:
        with open(path, "r+b") as f:
            mmapped_file = mmap.mmap(f.fileno(), 0)
            mmapped_file.seek(start_position)
            jsonStream = getFileJsonStream(path, mmapped_file)
            if jsonStream is None:
                print(f"Skipping unknown file {path}")
                return start_position
            progressLog = FileProgressLog(path, mmapped_file)

            batch = []
            for row in jsonStream:
                try:
                    progressLog.onRow()
                    batch.append(row)
                    
                    if len(batch) >= CHUNK_SIZE:
                        result = process_batch(batch)
                        for month_year, subreddits in result.items():
                            for subreddit, data in subreddits.items():
                                writer.write(month_year, subreddit, orjson.dumps(data) + b'\n')
                        batch = []
                except zstd.ZstdError as ze:
                    print(f"Zstd error in file {path} at position {mmapped_file.tell()}: {ze}")
                    mmapped_file.seek((mmapped_file.tell() + 3) & ~3)
                    continue
                except Exception as je:
                    print(f"JSON decode error in file {path} at position {mmapped_file.tell()}: {je}")
                    continue

            # Process any remaining data
            if batch:
                result = process_batch(batch)
                for month_year, subreddits in result.items():
                    for subreddit, data in subreddits.items():
                        writer.write(month_year, subreddit, orjson.dumps(data) + b'\n')

            if progressLog.i > 0:
                progressLog.logProgress(force_print=True)
                print()  # New line after progress
            else:
                print(f"No rows processed in file: {path}")

            return mmapped_file.tell()
    except Exception as e:
        print(f"Error processing file {path}: {e}")
        return start_position  # Return the start position if an error occurs

def process_files(files, start_positions):
    writer = BufferedWriter(output_dir)
    for file, start_position in zip(files, start_positions):
        final_position = process_file(file, writer, start_position)
        yield file, final_position
    writer.close()

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'rb') as f:
            return pickle.load(f)
    return {}

def main():
    checkpoint = load_checkpoint()

    os.makedirs(output_dir, exist_ok=True)

    if os.path.isdir(fileOrFolderPath):
        files = []
        if recursive:
            for root, _, filenames in os.walk(fileOrFolderPath):
                files.extend(os.path.join(root, filename) for filename in filenames)
        else:
            files = [os.path.join(fileOrFolderPath, f) for f in os.listdir(fileOrFolderPath) if os.path.isfile(os.path.join(fileOrFolderPath, f))]
        
        files = [f for f in files if f.endswith('.zst')]
        start_positions = [checkpoint.get(f, 0) for f in files]
        
        for file, final_position in process_files(files, start_positions):
            checkpoint[file] = final_position
            
        with open(CHECKPOINT_FILE, 'wb') as f:
            pickle.dump(checkpoint, f)
    else:
        start_position = checkpoint.get(fileOrFolderPath, 0)
        writer = BufferedWriter(output_dir)
        final_position = process_file(fileOrFolderPath, writer, start_position)
        writer.close()
        checkpoint[fileOrFolderPath] = final_position
        with open(CHECKPOINT_FILE, 'wb') as f:
            pickle.dump(checkpoint, f)
    
    print("Processing complete!")

if __name__ == "__main__":
    mp.freeze_support()  # This is necessary for Windows support
    main()
