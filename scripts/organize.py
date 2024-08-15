import sys
import csv
import os
import json
import re
from typing import Dict
import zstandard as zstd
import multiprocessing as mp
from queue import Empty
from datetime import datetime

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

CHUNK_SIZE = 1000

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

NUM_PROCESSES = mp.cpu_count()  # Use all available CPU cores

def sanitize_subreddit_name(name: str) -> str:
    sanitized = re.sub(r'[^\w\-]', '', name)
    return sanitized[:50]

def get_month_year(created_utc: int) -> str:
    date = datetime.utcfromtimestamp(created_utc)
    return date.strftime("%Y-%m")

def write_jsonl_chunk(month_year: str, subreddit: str, data: list):
    sanitized_subreddit = sanitize_subreddit_name(subreddit)
    if not sanitized_subreddit:
        print(f"Skipping problematic subreddit name: {subreddit}")
        return
    
    month_dir = os.path.join(output_dir, month_year)
    os.makedirs(month_dir, exist_ok=True)
    output_file = os.path.join(month_dir, f"{sanitized_subreddit}.jsonl")
    with open(output_file, 'a', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')

def compress_to_zst(input_file: str, output_file: str):
    with open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            compressor = zstd.ZstdCompressor(level=3)
            compressor.copy_stream(f_in, f_out)

def process_chunk(chunk: Dict[str, Dict[str, list]]):
    for month_year, subreddits in chunk.items():
        for subreddit, data in subreddits.items():
            if data:
                write_jsonl_chunk(month_year, subreddit, data)
    chunk.clear()

def process_file(path: str, queue: mp.Queue):
    print(f"Processing file {path}")
    chunk = {}
    row_count = 0
    
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        progressLog = FileProgressLog(path, f)
        
        for row in jsonStream:
            progressLog.onRow()
            subreddit = row["subreddit"]
            created_utc = row["created_utc"]
            month_year = get_month_year(created_utc)
            
            if WHITELIST and subreddit not in WHITELIST:
                continue
            
            if month_year not in chunk:
                chunk[month_year] = {}
            if subreddit not in chunk[month_year]:
                chunk[month_year][subreddit] = []
            
            chunk[month_year][subreddit].append(row)
            row_count += 1
            
            if row_count >= CHUNK_SIZE:
                queue.put(chunk)
                chunk = {}
                row_count = 0
        
        # Process any remaining data
        if chunk:
            queue.put(chunk)
        
        progressLog.logProgress("\n")

def writer_process(queue: mp.Queue):
    while True:
        try:
            chunk = queue.get(timeout=1)
            if chunk is None:
                break
            process_chunk(chunk)
        except Empty:
            continue

def process_files(files: list):
    with mp.Manager() as manager:
        queue = manager.Queue(maxsize=10 * NUM_PROCESSES)  # Increase queue size
        
        writers = [mp.Process(target=writer_process, args=(queue,)) for _ in range(NUM_PROCESSES // 2)]
        for writer in writers:
            writer.start()

        with mp.Pool(NUM_PROCESSES // 2) as pool:
            pool.starmap(process_file, [(file, queue) for file in files])

        for _ in writers:
            queue.put(None)  # Signal writer processes to finish

        for writer in writers:
            writer.join()

def compress_output_files():
    with mp.Pool(NUM_PROCESSES) as pool:
        compression_tasks = []
        for month_year in os.listdir(output_dir):
            month_dir = os.path.join(output_dir, month_year)
            if os.path.isdir(month_dir):
                for filename in os.listdir(month_dir):
                    if filename.endswith('.jsonl'):
                        input_file = os.path.join(month_dir, filename)
                        output_file = os.path.join(month_dir, f"{os.path.splitext(filename)[0]}.zst")
                        compression_tasks.append((input_file, output_file))
        
        pool.starmap(compress_to_zst, compression_tasks)
        
        for input_file, _ in compression_tasks:
            os.remove(input_file)  # Remove the original jsonl file after compression

def main():
    if os.path.isdir(fileOrFolderPath):
        files = []
        if recursive:
            for root, _, filenames in os.walk(fileOrFolderPath):
                files.extend(os.path.join(root, filename) for filename in filenames)
        else:
            files = [os.path.join(fileOrFolderPath, f) for f in os.listdir(fileOrFolderPath) if os.path.isfile(os.path.join(fileOrFolderPath, f))]
        process_files(files)
    else:
        process_files([fileOrFolderPath])
    
    compress_output_files()
    print("Done :>")

if __name__ == "__main__":
    mp.freeze_support()  # This is necessary for Windows support
    main()