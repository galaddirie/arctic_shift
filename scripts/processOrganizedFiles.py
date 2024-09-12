import sys
import csv
import os
from typing import Iterable, Dict, List, Set, Optional
from fileStreams import getFileJsonStream
from utils import FileProgressLog
from datetime import datetime

version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")

# Constants
PROCESSING_COMMENTS = False  # Set to False if processing posts
process_in_reverse = True
fileOrFolderPath = r"D:\reddit\dumps\reddit\submissions\test"

# Subreddits and search terms
subreddits = ["mcdonalds", "askacanadian", "canadaimmigrant", "mcdonaldsemployees"]
search_terms: Dict[str, Optional[List[str]]] = {
    "mcdonalds": ["Canada", "Canadian"],
    "askacanadian": ["McDonalds", "McDonald's"],
    "canadaimmigrant": ["McDonalds", "McDonald's"],
    "mcdonaldsemployees": None,  # No specific search terms for mcdonaldsemployees
}

# CSV writers and processed IDs
csv_writers: Dict[str, csv.DictWriter] = {}
processed_ids: Dict[str, Set[str]] = {}

def create_csv_writer(subreddit: str, term: Optional[str] = None):
    output_csv = f"reddit_{subreddit}_{'comments' if PROCESSING_COMMENTS else 'posts'}{f'_{term.replace(' ', '_')}' if term else ''}.csv"
    file = open(output_csv, 'w', newline='', encoding='utf-8')
    fieldnames = ['post_id', 'post_title','post_text', 'post_comment_count', 'post_url', 'post_date', 'poster_username']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()  # Write the header
    return writer

def process_row(row: Dict, subreddit: str, term: Optional[str] = None):
    csv_key = f"{subreddit}_{term}" if term else subreddit
    if row['id'] not in processed_ids[csv_key]:
        post_id = row.get("link_id", "") if PROCESSING_COMMENTS else row.get("id", "")
        post_title = row.get("title", "")
        post_url = row.get("url", "")
        post_comment_count = row.get("num_comments", 0)
        post_text = row.get("body", "") if PROCESSING_COMMENTS else row.get("selftext", "")
        post_date = datetime.fromtimestamp(row.get("created_utc", 0)).strftime('%Y-%m-%d %H:%M:%S')
        poster_username = row.get("author", "")

        csv_writers[csv_key].writerow({
            'post_id': post_id,
            'post_title': post_title,
            'post_url': post_url,
            'post_comment_count': post_comment_count,
            'post_text': post_text,
            'post_date': post_date,
            'poster_username': poster_username
        })
        processed_ids[csv_key].add(row['id'])

def process_file(path: str):
    print(f"Processing file {path}")
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        progressLog = FileProgressLog(path, f)
        for row in jsonStream:
            progressLog.onRow()
            subreddit = row["subreddit"].lower()
            if subreddit in subreddits:
                terms = search_terms[subreddit]
                if terms is None:
                    # Process all data for subreddits without specific search terms
                    csv_key = subreddit
                    if csv_key not in csv_writers:
                        csv_writers[csv_key] = create_csv_writer(subreddit)
                        processed_ids[csv_key] = set()
                    process_row(row, subreddit)
                else:
                    text_to_search = row.get("body" if PROCESSING_COMMENTS else "selftext", "").lower()
                    for term in terms:
                        if term.lower() in text_to_search or term.lower() in row.get("title", "").lower():
                            csv_key = f"{subreddit}_{term}"
                            if csv_key not in csv_writers:
                                csv_writers[csv_key] = create_csv_writer(subreddit, term)
                                processed_ids[csv_key] = set()
                            process_row(row, subreddit, term)
        progressLog.logProgress("\n")

def process_folder(path: str):
    for year_month in sorted(os.listdir(path), reverse=process_in_reverse):
        year_month_path = os.path.join(path, year_month)
        if os.path.isdir(year_month_path):
            print(f"Processing folder: {year_month}")
            for subreddit_file in os.listdir(year_month_path):
                print(f"Processing file: {subreddit_file}")
                if subreddit_file.lower().rstrip('.zml') in subreddits:
                    file_path = os.path.join(year_month_path, subreddit_file)
                    process_file(file_path)

def main():
    if os.path.isdir(fileOrFolderPath):
        process_folder(fileOrFolderPath)
    else:
        process_file(fileOrFolderPath)
    
    # Close all CSV files
    for writer in csv_writers.values():
        writer.file.close()
    
    print("Done :>")

if __name__ == "__main__":
    main()