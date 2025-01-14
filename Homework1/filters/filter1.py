"""
filter1.py

Implements the first filter in the pipeline, which fetches publisher codes,
parses them, and saves them to publishers.db.

Then calls Filter2.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
from pathlib import Path

# ABSOLUTE IMPORT from our "Homework1.filters" package:
from Homework1.filters.base_filter import BaseFilter

class Filter1(BaseFilter):
    """
    Filter1 is responsible for:
      1) Fetching publisher codes (scraping from MSE dropdown).
      2) Parsing that HTML to extract valid alpha-only publisher codes.
      3) Saving them to publishers.db.
      4) Calling Filter2 afterwards.
    """

    def __init__(self):
        super().__init__()
        self.THIS_FOLDER = Path(__file__).parent.resolve()
        self.HOMEWORK3_PATH = self.THIS_FOLDER.parent.parent / "Homework3"
        self.db_path = self.HOMEWORK3_PATH / "publishers.db"

    def setup(self):
        print("Filter1 setup: Ensuring DB path is configured...")

    def scrape_data(self):
        url = 'https://www.mse.mk/mk/stats/symbolhistory/avk'
        response = requests.get(url)
        if response.status_code != 200:
            print("Filter1: Failed to fetch issuers from MSE.")
            return ""  # Return empty as "no data"
        print("Filter1: Successfully fetched issuer dropdown HTML.")
        return response.text

    def parse_data(self, raw_html):
        if not raw_html:
            return []

        soup = BeautifulSoup(raw_html, 'html.parser')
        dropdown = soup.find('select', {'id': 'Code'})

        if not dropdown:
            print("Filter1: No 'Code' <select> found in HTML.")
            return []

        # Extract alpha-only values
        publisher_codes = [
            option.get('value')
            for option in dropdown.find_all('option')
            if option.get('value') and option.get('value').isalpha()
        ]

        unique_codes = list(set(publisher_codes))
        print(f"Filter1: Extracted {len(unique_codes)} unique publisher codes.")
        return unique_codes

    def save_data(self, publisher_codes):
        if not publisher_codes:
            print("Filter1: No codes to save.")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS publishers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                publisher_code TEXT UNIQUE
            )
        ''')
        # Clear old data (optional)
        cursor.execute("DELETE FROM publishers")
        for code in publisher_codes:
            cursor.execute(
                "INSERT OR IGNORE INTO publishers (publisher_code) VALUES (?)",
                (code,)
            )
        conn.commit()
        conn.close()
        print("Filter1: Saved publisher codes to publishers.db.")

    def call_next_filter(self):
        print("Filter1: Calling Filter2 next...")
        # ABSOLUTE import of Filter2:
        from Homework1.filters.filter2 import Filter2

        filter2 = Filter2()
        filter2.run()


def main():
    f1 = Filter1()
    f1.run()

if __name__ == '__main__':
    main()
