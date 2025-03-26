# S3 Daily CSV Merger

This script aggregates daily CSV backup reports stored in an S3 bucket within a specified date range and produces a single consolidated CSV file.

---

## Table of Contents
- [S3 Daily CSV Merger](#s3-daily-csv-merger)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [How It Works](#how-it-works)
  - [Troubleshooting](#troubleshooting)
  - [License](#license)

---

## Overview

Many AWS Backup or custom backup services generate daily CSV reports and place them in an S3 bucket following a structured path (e.g., `s3://<bucket>/<prefix>/YYYY/MM/DD/`). This script iterates through each day in a specified date range, finds any CSV files, and merges all rows into a single CSV file locally.

---

## Features
- **Date-by-date scanning**: Loops through each day from a start date to an end date.
- **Automatic CSV detection**: Only merges files ending with `.csv`; other file types are ignored.
- **Dynamic columns**: If different CSVs contain different columns, the final CSV includes them all.
- **Single consolidated report**: Outputs a single `.csv` that combines all rows from the entire date range.

---

## Prerequisites

1. **Python 3.7+**  
   The script uses Python’s standard libraries plus `boto3`.

2. **AWS Credentials**  
   You must have valid AWS credentials configured to allow S3 read access. The easiest way is to run `aws configure` and ensure you have the correct `[default]` profile, or set up environment variables.

3. **boto3 Library**  
   Install via `pip install boto3` if you don’t already have it.

---

## Usage

1. **Clone or copy** the script to your local machine.
2. **Update script variables**:
   - `S3_BUCKET`: The name of the bucket containing your daily CSV folders.
   - `BASE_PREFIX`: The common prefix path in that bucket, e.g., `Backup/12345/us-east-1`.
   - `START_DATE` / `END_DATE`: The date range you want to merge (format `datetime(YYYY, MM, DD)`).
   - `OUTPUT_CSV`: The name of the local CSV file where all data will be combined.
3. **Run the script**:
   ```bash
   python backup_report.py
   ```
   Where `backup_report.py` is the name of the script file.

4. **Review the results**:
   - The script logs each day’s prefix it checks.
   - It then combines all the CSV rows found into `OUTPUT_CSV` locally.
   - If no CSV files or rows were found in the date range, an empty CSV is created.

---

## Configuration

Here are the main variables you may want to edit in the script:

- **`S3_BUCKET`**  
  Bucket name hosting your daily CSV reports.  
  Example: `"backup-reporting"`.

- **`BASE_PREFIX`**  
  The path under your bucket where daily folders reside.  
  Example: `"Backup/12345/us-east-1"`.

- **`START_DATE` / `END_DATE`**  
  A Python `datetime` object specifying start and end of the iteration.  
  Example: `START_DATE = datetime(2024, 10, 1)`  
           `END_DATE   = datetime(2025,  2, 28)`

- **`OUTPUT_CSV`**  
  The local filename to which the script writes the merged data.  
  Example: `"all_backup_data_merged.csv"`.

---

## How It Works

1. **Daily Prefix Calculation**  
   For each date in `[START_DATE, END_DATE]`, the script constructs a prefix like:
   ```
   {BASE_PREFIX}/{YYYY}/{MM}/{DD}/
   ```
   For example: `Backup/12345/us-east-1/2024/10/01/`.

2. **Listing S3 Objects**  
   The script calls `list_objects_v2` for that prefix, collecting any keys that end with `.csv`. Sub-folder placeholders or other file types are ignored.

3. **Parsing Each CSV**  
   Each CSV is downloaded into memory using `boto3` (`get_object`) and parsed via Python’s `csv.DictReader`.

4. **Union of Columns**  
   Because columns can differ among CSVs, the script dynamically tracks each unique column name found. In the final output, missing columns are left blank as needed.

5. **Writing the Final CSV**  
   After processing all dates, the script writes a single CSV file. The columns are the union of all columns discovered, and all rows from the entire range are included.

---

## Troubleshooting

- **No Rows in Final CSV**:  
  Double-check your `START_DATE`, `END_DATE`, `S3_BUCKET`, and `BASE_PREFIX` are correct. Also confirm the script has permission to read from S3.

- **Permission Denied**:  
  Ensure your AWS credentials or IAM role allows `s3:GetObject` and `s3:ListBucket` for the specified bucket/prefix.

- **Large Data**:  
  If you have **very large** CSVs or many days, you might need more memory or a more robust environment. Otherwise, consider streaming solutions or partial merges.

- **Date Range**:  
  The script uses a day-by-day loop. Double-check you haven’t reversed `START_DATE` and `END_DATE`.
