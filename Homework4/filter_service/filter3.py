# Homework4/filter_service/filter3.py

"""
filter3.py
Final filter: reads last_dates.json, fetches leftover data, merges into stock_data.db.
Concurrency=2. No deletion.
"""

import sqlite3
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from base_filter import BaseFilter

class Filter3(BaseFilter):
    def __init__(self):
        super().__init__()
        self.THIS_FOLDER = Path(__file__).parent.resolve()
        self.DB_PATH = self.THIS_FOLDER.parent / "stock_data.db"
        self.LAST_DATES_JSON = self.THIS_FOLDER / "last_dates.json"
        self.BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'

    def scrape_data(self):
        try:
            with open(self.LAST_DATES_JSON, 'r') as jf:
                last_dates = json.load(jf)
        except FileNotFoundError:
            print("Filter3: No last_dates.json found.")
            return {}
        return last_dates

    def parse_data(self, last_dates):
        if not last_dates:
            print("Filter3: No last dates to process.")
            return {}

        final_data = {}

        def process_publisher(code, from_str):
            new_records = []
            from_dt = datetime.strptime(from_str, '%d.%m.%Y') + timedelta(days=1)
            to_dt = datetime.now()
            while from_dt < to_dt:
                end_dt = min(from_dt + timedelta(days=365), to_dt)
                params = {
                    'FromDate': from_dt.strftime('%d.%m.%Y'),
                    'ToDate': end_dt.strftime('%d.%m.%Y'),
                    'Code': code
                }
                resp = requests.get(self.BASE_URL + code, params=params)
                if resp.status_code==200:
                    chunk = self._parse_stock_table(resp.text)
                    for rec in chunk:
                        if self._compare_dates(rec["Date"], from_str)>0:
                            new_records.append(rec)
                from_dt = end_dt + timedelta(days=1)
            return (code, new_records)

        # concurrency=2
        with ThreadPoolExecutor(max_workers=2) as exe:
            fut_map = {
                exe.submit(process_publisher, c, last_dates[c]): c
                for c in last_dates
            }
            for fut in as_completed(fut_map):
                cd = fut_map[fut]
                try:
                    pcode, recs = fut.result()
                    final_data[pcode] = recs
                except Exception as e:
                    print(f"Filter3: Error for {cd}: {e}")

        return final_data

    def _compare_dates(self, d1_str, d2_str):
        fmt = '%d.%m.%Y'
        d1 = datetime.strptime(d1_str, fmt)
        d2 = datetime.strptime(d2_str, fmt)
        return (d1 - d2).days

    def _parse_stock_table(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {'id':'resultsTable'})
        if not table:
            return []
        rows = table.find_all('tr')[1:]
        data = []
        for row in rows:
            cols = row.find_all('td')
            if len(cols)<9:
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
        return val.replace('\xa0','').strip()

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
        c = conn.cursor()
        total_new = 0
        for code, recs in final_data.items():
            for r in recs:
                c.execute("""
                    INSERT OR REPLACE INTO stock_data (
                        publisher_code, date, price, max, min, avg,
                        percent_change, quantity, best_turnover, total_turnover
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code,
                    r["Date"],
                    r["Price"],
                    r["Max"],
                    r["Min"],
                    r["Avg"],
                    r["Percent Change"],
                    r["Quantity"],
                    r["Best Turnover"],
                    r["Total Turnover"]
                ))
                total_new += 1
        conn.commit()
        conn.close()
        print(f"Filter3: Inserted {total_new} new rows (no wipe).")

def main():
    f3 = Filter3()
    f3.run()

if __name__=="__main__":
    main()
