import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Dict

from fileStreams import getFileJsonStream  # Assuming these imports exist in your environment
from utils import FileProgressLog

import csv
from datetime import datetime

fileOrFolderPath = r"D:\reddit\dumps\reddit\submissions"
recursive = False

subreddit = "PersonalFinance"
# subreddit = subreddit.lower()
search_terms = ["credit card", "visa"]

comments = False

# Create a dictionary to store CSV writers and file objects for each search term
csv_writers: Dict[str, csv.writer] = {}
csv_files: Dict[str, open] = {}

def init_csv_files():
    for term in search_terms:
        output_csv = f"reddit_{subreddit}_{'comments' if comments else 'posts'}_{term.replace(' ', '_')}.csv"
        csv_file = open(output_csv, 'w', newline='', encoding='utf-8')
        csv_files[term] = csv_file
        csv_writers[term] = csv.writer(csv_file)
        csv_writers[term].writerow([
            "post_id", "post_title", "post_url", "post_comment_count", "post_text",
            "post_date", "poster_username", "subreddit_name", "search_term"
        ])

def close_csv_files():
    for csv_file in csv_files.values():
        csv_file.close()

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
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            month_folder = os.path.join(root, dir_name)
            print(f"Processing month folder {month_folder}")
            subreddit_file = os.path.join(month_folder, f"{subreddit}.zst")
            if os.path.exists(subreddit_file):
                print(f"Found {subreddit_file}, processing...")
                processFile(subreddit_file)
            else:
                print(f"Subreddit file {subreddit}.zst not found in {month_folder}")

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
