#!/usr/bin/env python3
"""
Upload a CSV to a Google Sheet.

Usage:
  python sheets_uploader.py --csv path/to/file.csv --sheet "Sheet Name" --mode append
  python sheets_uploader.py --csv path/to/file.csv --sheet "Sheet Name" --mode replace
"""

import argparse
import gspread
from google.oauth2.service_account import Credentials
import csv
import sys

def upload_csv_to_sheet(csv_file, sheet_name, mode):
    # Authenticate with service account
    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)

    # Open the sheet by name
    try:
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sys.exit(f"Spreadsheet '{sheet_name}' not found or service account not invited.")

    # Read CSV content
    with open(csv_file, newline='', encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if mode == "replace":
        sheet.clear()
        sheet.update("A1", rows)
        print(f"[OK] Replaced all data in '{sheet_name}' with {len(rows)} rows.")
    elif mode == "append":
        # Append after the last non-empty row
        existing_rows = len(sheet.get_all_values())
        start_row = existing_rows + 1
        range_str = f"A{start_row}"
        sheet.update(range_str, rows)
        print(f"[OK] Appended {len(rows)} rows to '{sheet_name}'.")
    else:
        sys.exit("Invalid mode. Use 'append' or 'replace'.")

def main():
    parser = argparse.ArgumentParser(description="Upload CSV to Google Sheets")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--sheet", required=True, help="Google Sheet name")
    parser.add_argument("--mode", choices=["append", "replace"], required=True, help="Upload mode")
    args = parser.parse_args()

    upload_csv_to_sheet(args.csv, args.sheet, args.mode)

if __name__ == "__main__":
    main()
