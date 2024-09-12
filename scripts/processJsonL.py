import sys
import csv
from datetime import datetime
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Iterable

from fileStreams import getFileJsonStream
from utils import FileProgressLog

fileOrFolderPath = r"D:\reddit\data\r_airtransat_posts.jsonl"
recursive = False
output_csv_path = "reddit_airtransat_posts.csv"

fieldnames = ['post_id', 'post_title', 'post_text', 'post_comment_count', 'post_url', 'post_date', 'poster_username', 'subreddit_name']

def processFile(path: str, csv_writer):
    print(f"Processing file {path}")
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        progressLog = FileProgressLog(path, f)
        for row in jsonStream:
            progressLog.onRow()
            
            print(row)
            
            # Map the row data to our field names
            mapped_row = {
                'post_id': row.get('id', ''),
                'post_title': row.get('title', ''),
                'post_url': row.get('url', '') if 'url' in row else row.get('permalink', ''),
                'post_text': row.get('selftext', '') if 'selftext' in row else row.get('body', ''),
                'post_comment_count': row.get('num_comments', '0'), 
                'post_date': datetime.fromtimestamp(row.get('created_utc', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'poster_username': row.get('author', ''),
                'subreddit_name': row.get("subreddit", "")

            }
            
            csv_writer.writerow(mapped_row)

        progressLog.logProgress("\n")

def processFolder(path: str, csv_writer):
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
        processFile(file, csv_writer)

def main():
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()

        if os.path.isdir(fileOrFolderPath):
            processFolder(fileOrFolderPath, csv_writer)
        else:
            processFile(fileOrFolderPath, csv_writer)
    
    print(f"Done :> CSV file created at {output_csv_path}")

if __name__ == "__main__":
    main()
