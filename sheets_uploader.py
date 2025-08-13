#!/usr/bin/env python3
"""
Upload a CSV to a Google Sheet.

Usage:
  python sheets_uploader.py --csv path/to/file.csv --sheet "Sheet Name" --mode append
  python sheets_uploader.py --csv path/to/file.csv --sheet "Sheet Name" --mode replace
"""

import argparse
import csv
import sys
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",  # needed for open() by name
]

def upload_csv_to_sheet(csv_file, sheet_name, mode):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    # open by name -> needs Drive scope
    try:
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sys.exit(f"Spreadsheet '{sheet_name}' not found or service account not invited.")

    # read CSV
    with open(csv_file, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if not rows:
        print("[WARN] CSV is empty. Nothing to upload.")
        return

    if mode == "replace":
        sheet.clear()
        sheet.update("A1", rows)
        print(f"[OK] Replaced all data in '{sheet_name}' with {len(rows)-1} data rows.")
    elif mode == "append":
        # skip header when appending
        header, data_rows = rows[0], rows[1:]
        if not data_rows:
            print("[INFO] CSV has only a header; nothing to append.")
            return

        # find next empty row
        existing = sheet.get_all_values()
        start_row = len(existing) + 1
        start_col = 1
        end_col = len(header)

        # compute A1 range
        def col_letter(n: int) -> str:
            s = ""
            while n > 0:
                n, r = divmod(n - 1, 26)
                s = chr(65 + r) + s
            return s

        rng = f"A{start_row}:{col_letter(end_col)}{start_row + len(data_rows) - 1}"
        sheet.update(rng, data_rows)
        print(f"[OK] Appended {len(data_rows)} rows to '{sheet_name}'.")
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
