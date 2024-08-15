import sys
import csv
import os
import orjson as json
import re
from typing import Dict, List
import zstandard as zstd
import multiprocessing as mp
from queue import Empty
from datetime import datetime, UTC
import pickle

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
CHUNK_SIZE = 1_000_000
CHECKPOINT_FILE = "checkpoint.pkl"
QUEUE_TIMEOUT = 30  # Increased timeout for queue operations

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
    date = datetime.fromtimestamp(created_utc, UTC)
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

def process_file(path: str, queue: mp.Queue, start_position: int = 0):
       print(f"Processing file {path} from position {start_position}")
       chunk = {}
       row_count = 0
       file_name = os.path.basename(path)
       if file_name in BLACKLISTED_FILES:
           print(f"Skipping blacklisted file: {file_name}")
           return start_position

       try:
           with open(path, "rb") as f:
               if start_position < 0:
                   print(f"Warning: Invalid start position {start_position} for file {path}. Starting from beginning.")
                   start_position = 0
               f.seek(start_position)
               jsonStream = getFileJsonStream(path, f)
               if jsonStream is None:
                   print(f"Skipping unknown file {path}")
                   return start_position
               progressLog = FileProgressLog(path, f)

               for row in jsonStream:
                   try:
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
                           queue.put((chunk, path, f.tell()))
                           chunk = {}
                           row_count = 0
                   except zstd.ZstdError as ze:
                       print(f"Zstd error in file {path} at position {f.tell()}: {ze}")
                       # Skip to the next 4-byte aligned position
                       f.seek((f.tell() + 3) & ~3)
                       continue
                   except json.JSONDecodeError as je:
                       print(f"JSON decode error in file {path} at position {f.tell()}: {je}")
                       continue

               # Process any remaining data
               if chunk:
                   queue.put((chunk, path, f.tell()))

               if progressLog.i > 0:
                   progressLog.logProgress("\n")
               else:
                   print(f"No rows processed in file: {path}")

               return f.tell()
       except Exception as e:
           print(f"Error processing file {path}: {e}")
           return start_position  # Return the start position if an error occurs

def writer_process(queue: mp.Queue, checkpoint_queue: mp.Queue):
    while True:
        try:
            item = queue.get(timeout=QUEUE_TIMEOUT)
            if item is None:
                break
            chunk, file_path, file_position = item
            process_chunk(chunk)
            checkpoint_queue.put((file_path, file_position))
        except Empty:
            continue
        except EOFError:
            print("Writer process encountered EOFError. Exiting.")
            break

def checkpoint_process(checkpoint_queue: mp.Queue):
    checkpoint = {}
    while True:
        try:
            item = checkpoint_queue.get(timeout=QUEUE_TIMEOUT)
            if item is None:
                break
            file_path, file_position = item
            checkpoint[file_path] = file_position
            with open(CHECKPOINT_FILE, 'wb') as f:
                pickle.dump(checkpoint, f)
        except Empty:
            continue
        except EOFError:
            print("Checkpoint process encountered EOFError. Exiting.")
            break

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'rb') as f:
            return pickle.load(f)
    return {}

def process_single_file(file: str, start_position: int = 0):
    with mp.Manager() as manager:
        queue = manager.Queue(maxsize=10 * NUM_PROCESSES)
        checkpoint_queue = manager.Queue()

        writers = [mp.Process(target=writer_process, args=(queue, checkpoint_queue)) for _ in range(NUM_PROCESSES)]
        for writer in writers:
            writer.start()

        checkpoint_proc = mp.Process(target=checkpoint_process, args=(checkpoint_queue,))
        checkpoint_proc.start()

        try:
            final_position = process_file(file, queue, start_position)
        except Exception as e:
            print(f"Error processing file {file}: {e}")
            final_position = start_position  # Set final_position to start_position if an error occurs
        finally:
            # Signal processes to finish
            for _ in writers:
                queue.put(None)
            checkpoint_queue.put(None)

        for writer in writers:
            writer.join()
        checkpoint_proc.join()

    return final_position

def compress_output_files():
    compression_tasks = []
    for month_year in os.listdir(output_dir):
        month_dir = os.path.join(output_dir, month_year)
        if os.path.isdir(month_dir):
            for filename in os.listdir(month_dir):
                if filename.endswith('.jsonl'):
                    input_file = os.path.join(month_dir, filename)
                    output_file = os.path.join(month_dir, f"{os.path.splitext(filename)[0]}.zst")
                    compress_to_zst(input_file, output_file)
                    os.remove(input_file)  # Remove the original jsonl file after compression

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
        
        for file in files:
            if file.endswith('.zst'):
                start_position = checkpoint.get(file, 0)
                final_position = process_single_file(file, start_position)
                checkpoint[file] = final_position
                compress_output_files()
    else:
        start_position = checkpoint.get(fileOrFolderPath, 0)
        process_single_file(fileOrFolderPath, start_position)
        compress_output_files()
    
    print("Done :>")

if __name__ == "__main__":
    mp.freeze_support()  # This is necessary for Windows support
    main()