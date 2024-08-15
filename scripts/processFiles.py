import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Iterable, List, Dict

from fileStreams import getFileJsonStream
from utils import FileProgressLog

import csv
from datetime import datetime

fileOrFolderPath = r"D:\reddit\dumps\reddit\submissions"
recursive = False

subreddit = "PersonalFinanceCanada"
search_terms = ["credit card", "visa"]

comments = False

# Create a dictionary to store CSV writers for each search term
csv_writers: Dict[str, csv.writer] = {}

def init_csv_files():
    for term in search_terms:
        output_csv = f"reddit_{subreddit}_{'comments' if comments else 'posts'}_{term.replace(' ', '_')}.csv"
        csv_file = open(output_csv, 'w', newline='', encoding='utf-8')
        csv_writers[term] = csv.writer(csv_file)
        csv_writers[term].writerow([
            "post_id", "post_title", "post_url", "post_comment_count", "post_text",
            "post_date", "poster_username", "subreddit_name", "search_term"
        ])

def close_csv_files():
    for csv_file in csv_writers.values():
        csv_file.writerow.close()

def processFile(path: str):
    print(f"Processing file {path}")
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        progressLog = FileProgressLog(path, f)
        for row in jsonStream:
            progressLog.onRow()
            
            if subreddit.lower() in row.get("subreddit", "").lower():
                post_id = row.get("id", "")
                post_title = row.get("title", "")
                post_url = row.get("url", "")
                post_comment_count = row.get("num_comments", 0)
                post_text = row.get("selftext", "")
                post_date = datetime.fromtimestamp(row.get("created_utc", 0)).strftime('%Y-%m-%d %H:%M:%S')
                poster_username = row.get("author", "")
                subreddit_name = row.get("subreddit", "")
                
                if comments:
                    post_text = row.get("body", "")
                    post_id = row.get("link_id", "")
                
                for term in search_terms:
                    if term.lower() in post_title.lower() or term.lower() in post_text.lower():
                        csv_writers[term].writerow([
                            post_id, post_title, post_url, post_comment_count, post_text,
                            post_date, poster_username, subreddit_name, term
                        ])
        
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

def main():
    init_csv_files()
    
    if os.path.isdir(fileOrFolderPath):
        processFolder(fileOrFolderPath)
    else:
        processFile(fileOrFolderPath)
    
    close_csv_files()
    print("Done :>")

if __name__ == "__main__":
    main()