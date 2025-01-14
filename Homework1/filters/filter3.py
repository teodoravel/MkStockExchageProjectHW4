"""
filter3.py

Final filter that fetches/formats any missing data,
fills in consistent formatting, merges into stock_data.db.
Typically the last step in the chain.
"""

import sqlite3
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# ABSOLUTE IMPORT:
from Homework1.filters.base_filter import BaseFilter

class Filter3(BaseFilter):
    """
    Filter3:
      1) Reads 'last_dates.json' for final missing data if needed
      2) Scrapes leftover data
      3) Formats prices/dates consistently
      4) Saves to stock_data.db
    """

    def __init__(self):
        super().__init__()
        self.THIS_FOLDER = Path(__file__).parent.resolve()
        self.HOMEWORK3_PATH = self.THIS_FOLDER.parent.parent / "Homework3"
        self.DB_PATH = self.HOMEWORK3_PATH / "stock_data.db"
        self.LAST_DATES_JSON = self.THIS_FOLDER / "last_dates.json"
        self.BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'

    def scrape_data(self):
        try:
            with open(self.LAST_DATES_JSON, 'r') as json_file:
                last_dates = json.load(json_file)
        except FileNotFoundError:
            print("Filter3: No last_dates.json found. Possibly nothing to do.")
            return {}
        return last_dates

    def parse_data(self, last_dates):
        if not last_dates:
            print("Filter3: No last dates to process. Probably done.")
            return {}

        final_data = {}

        def process_publisher(publisher_code, from_date_str):
            new_records = []
            from_datetime = datetime.strptime(from_date_str, '%d.%m.%Y') + timedelta(days=1)
            to_datetime = datetime.now()
            while from_datetime < to_datetime:
                end_datetime = min(from_datetime + timedelta(days=365), to_datetime)
                params = {
                    'FromDate': from_datetime.strftime('%d.%m.%Y'),
                    'ToDate': end_datetime.strftime('%d.%m.%Y'),
                    'Code': publisher_code
                }
                resp = requests.get(self.BASE_URL + publisher_code, params=params)
                if resp.status_code == 200:
                    chunk_records = self._parse_stock_table(resp.text)
                    for rec in chunk_records:
                        if self._compare_dates(rec['Date'], from_date_str) > 0:
                            new_records.append(rec)
                from_datetime = end_datetime + timedelta(days=1)
            return (publisher_code, new_records)

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_code = {
                executor.submit(process_publisher, code, last_dates[code]): code
                for code in last_dates
            }
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    (pcode, recs) = future.result()
                    final_data[pcode] = recs
                except Exception as e:
                    print(f"Filter3: Exception for {code}: {e}")

        return final_data

    def _compare_dates(self, date_str, compare_str):
        fmt = '%d.%m.%Y'
        d1 = datetime.strptime(date_str, fmt)
        d2 = datetime.strptime(compare_str, fmt)
        return (d1 - d2).days

    def _parse_stock_table(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {'id': 'resultsTable'})
        if not table:
            return []
        rows = table.find_all('tr')[1:]
        data = []
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 9:
                continue
            data.append({
                'Date': self._format_date(cols[0].text.strip()),
                'Price': self._format_price(cols[1].text.strip()),
                'Max': self._format_price(cols[2].text.strip()),
                'Min': self._format_price(cols[3].text.strip()),
                'Avg': self._format_price(cols[4].text.strip()),
                'Percent Change': cols[5].text.strip(),
                'Quantity': cols[6].text.strip(),
                'Best Turnover': self._format_price(cols[7].text.strip()),
                'Total Turnover': self._format_price(cols[8].text.strip())
            })
        return data

    def _format_price(self, val):
        return val.replace('\xa0', '').strip()

    def _format_date(self, date_str):
        try:
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return dt.strftime('%d.%m.%Y')
        except ValueError:
            return date_str

    def save_data(self, final_data):
        if not final_data:
            print("Filter3: No new data to save.")
            return

        conn = sqlite3.connect(self.DB_PATH)
        cursor = conn.cursor()
        new_data_count = 0

        for publisher_code, records in final_data.items():
            for record in records:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_data (
                        publisher_code, date, price, max, min, avg,
                        percent_change, quantity, best_turnover, total_turnover
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    publisher_code,
                    record['Date'],
                    record['Price'],
                    record['Max'],
                    record['Min'],
                    record['Avg'],
                    record['Percent Change'],
                    record['Quantity'],
                    record['Best Turnover'],
                    record['Total Turnover']
                ))
                new_data_count += 1
        conn.commit()
        conn.close()
        print(f"Filter3: Saved {new_data_count} new rows to stock_data.db.")


def main():
    f3 = Filter3()
    f3.run()

if __name__ == '__main__':
    main()
