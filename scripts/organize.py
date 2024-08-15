import sys
import os
import json
from typing import Iterable, Dict
import zstandard as zstd

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# Ensure Python 3.10+
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")

fileOrFolderPath = r"<path to file or folder>"
recursive = False
output_dir = r"<path to output directory>"
CHUNK_SIZE = 10000  # Number of rows to process before writing to disk
WHITELIST = set()  # Add your whitelist of subreddits here, e.g., {'AskReddit', 'funny', 'gaming'}

def write_jsonl_chunk(subreddit: str, data: list):
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{subreddit}.jsonl")
    with open(output_file, 'a', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')

def compress_to_zst(input_file: str, output_file: str):
    with open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            compressor = zstd.ZstdCompressor(level=3)  # Adjust compression level as needed
            compressor.copy_stream(f_in, f_out)

def process_chunk(chunk: Dict[str, list]):
    for subreddit, data in chunk.items():
        if data:
            write_jsonl_chunk(subreddit, data)
    chunk.clear()

def processFile(path: str):
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
            
            if WHITELIST and subreddit not in WHITELIST:
                continue
            
            if subreddit not in chunk:
                chunk[subreddit] = []
            
            chunk[subreddit].append(row)
            row_count += 1
            
            if row_count >= CHUNK_SIZE:
                process_chunk(chunk)
                row_count = 0
        
        # Process any remaining data
        process_chunk(chunk)
        
        progressLog.logProgress("\n")

def processFolder(path: str):
    fileIterator: Iterable[str]
    if recursive:
        def recursiveFileIterator():
            for root, dirs, files in os.walk(path):
                for file in files:
                    yield os.path.join(root, file)
        fileIterator = recursiveFileIterator()
    else:
        fileIterator = os.listdir(path)
        fileIterator = (os.path.join(path, file) for file in fileIterator)
    
    for i, file in enumerate(fileIterator):
        print(f"Processing file {i+1: 3} {file}")
        processFile(file)

def compress_output_files():
    for filename in os.listdir(output_dir):
        if filename.endswith('.jsonl'):
            input_file = os.path.join(output_dir, filename)
            output_file = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.zst")
            compress_to_zst(input_file, output_file)
            os.remove(input_file)  # Remove the original jsonl file after compression

def main():
    if os.path.isdir(fileOrFolderPath):
        processFolder(fileOrFolderPath)
    else:
        processFile(fileOrFolderPath)
    
    compress_output_files()
    print("Done :>")

if __name__ == "__main__":
    main()
