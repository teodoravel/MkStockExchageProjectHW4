# Homework4/filter_service/filter1.py

"""
filter1.py
Fetches publisher codes (MSE dropdown), saves them to publishers.db.
Then calls Filter2.

We've COMMENTED OUT the line that deletes the publishers table.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
from pathlib import Path

from base_filter import BaseFilter

class Filter1(BaseFilter):
    def __init__(self):
        super().__init__()
        self.THIS_FOLDER = Path(__file__).parent.resolve()
        # publishers.db in the parent Homework4 folder
        self.db_path = self.THIS_FOLDER.parent / "publishers.db"

    def setup(self):
        print("Filter1 setup: Ensuring DB path is configured...")

    def scrape_data(self):
        url = 'https://www.mse.mk/mk/stats/symbolhistory/avk'
        resp = requests.get(url)
        if resp.status_code != 200:
            print("Filter1: Failed to fetch MSE dropdown.")
            return ""
        print("Filter1: Successfully fetched issuer dropdown HTML.")
        return resp.text

    def parse_data(self, raw_html):
        if not raw_html:
            return []

        soup = BeautifulSoup(raw_html, 'html.parser')
        dropdown = soup.find('select', {'id': 'Code'})
        if not dropdown:
            print("Filter1: No 'Code' <select> found in HTML.")
            return []

        # alpha-only codes only
        codes = [
            opt.get('value')
            for opt in dropdown.find_all('option')
            if opt.get('value') and opt.get('value').isalpha()
        ]
        unique_codes = list(set(codes))
        print(f"Filter1: Extracted {len(unique_codes)} unique publisher codes.")
        return unique_codes

    def save_data(self, publisher_codes):
        if not publisher_codes:
            print("Filter1: No codes to save.")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS publishers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                publisher_code TEXT UNIQUE
            )
        """)

        # Originally: cursor.execute("DELETE FROM publishers")
        # COMMENTED OUT:
        # cursor.execute("DELETE FROM publishers")

        for code in publisher_codes:
            cursor.execute(
                "INSERT OR IGNORE INTO publishers (publisher_code) VALUES (?)",
                (code,)
            )
        conn.commit()
        conn.close()
        print("Filter1: Inserted publisher codes (no wipe).")

    def call_next_filter(self):
        print("Filter1: Calling Filter2 next...")
        from filter2 import Filter2
        f2 = Filter2()
        f2.run()

def main():
    f1 = Filter1()
    f1.run()

if __name__ == "__main__":
    main()
