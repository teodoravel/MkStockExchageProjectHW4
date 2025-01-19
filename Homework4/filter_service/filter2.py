# Homework4/filter_service/filter2.py

"""
filter2.py
Checks the last date for each publisher, scrapes missing data,
saves to stock_data.db, calls Filter3. Concurrency is set to 2.
"""

import requests
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from base_filter import BaseFilter

class Filter2(BaseFilter):
    def __init__(self):
        super().__init__()
        self.THIS_FOLDER = Path(__file__).parent.resolve()
        self.PUBLISHERS_DB = self.THIS_FOLDER.parent / "publishers.db"
        self.STOCK_DB = self.THIS_FOLDER.parent / "stock_data.db"
        self.BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'
        self.LAST_DATES_JSON = self.THIS_FOLDER / "last_dates.json"

    def setup(self):
        print("Filter2 setup: Concurrency=2, won't wipe anything.")

    def scrape_data(self):
        # fetch all publishers
        conn = sqlite3.connect(self.PUBLISHERS_DB)
        c = conn.cursor()
        c.execute("SELECT publisher_code FROM publishers")
        publisher_codes = [row[0] for row in c.fetchall()]
        conn.close()

        if not publisher_codes:
            print("Filter2: No publisher codes found in publishers.db.")
            return []

        results = []

        def fetch_for_publisher(code):
            return (code, self._fetch_publisher_data(code))

        # concurrency=2 (not 5)
        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_map = {
                executor.submit(fetch_for_publisher, code): code
                for code in publisher_codes
            }
            for fut in as_completed(fut_map):
                cd = fut_map[fut]
                try:
                    result = fut.result()
                    results.append(result)
                except Exception as e:
                    print(f"Filter2: Exception for {cd}: {e}")

        return results

    def _fetch_publisher_data(self, publisher_code):
        last_date_in_db = self._get_last_data_date(publisher_code)
        if last_date_in_db:
            from_dt = datetime.strptime(last_date_in_db, '%d.%m.%Y') + timedelta(days=1)
            print(f"Filter2: {publisher_code} has data up to {last_date_in_db}, fetching more.")
        else:
            from_dt = datetime.now() - timedelta(days=3650)
            print(f"Filter2: {publisher_code} no data, fetch 10 years.")
        to_dt = datetime.now()
        combined_html = []

        while from_dt < to_dt:
            end_date = min(from_dt + timedelta(days=365), to_dt)
            params = {
                'FromDate': from_dt.strftime('%d.%m.%Y'),
                'ToDate': end_date.strftime('%d.%m.%Y'),
                'Code': publisher_code
            }
            url = self.BASE_URL + publisher_code
            resp = requests.get(url, params=params)
            if resp.status_code == 200:
                combined_html.append(resp.text)
            else:
                print(f"Filter2: {publisher_code} HTTP {resp.status_code} from {from_dt} to {end_date}")
            from_dt = end_date + timedelta(days=1)

        return (publisher_code, combined_html)

    def _get_last_data_date(self, publisher_code):
        conn = sqlite3.connect(self.STOCK_DB)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                publisher_code TEXT,
                date TEXT,
                price TEXT,
                max TEXT,
                min TEXT,
                avg TEXT,
                percent_change TEXT,
                quantity TEXT,
                best_turnover TEXT,
                total_turnover TEXT,
                UNIQUE(publisher_code, date) ON CONFLICT REPLACE
            )
        """)
        c.execute("SELECT MAX(date) FROM stock_data WHERE publisher_code=?", (publisher_code,))
        last_dt = c.fetchone()[0]
        conn.close()
        return last_dt

    def parse_data(self, scraped_results):
        parsed_dict = {}
        last_dates = {}
        for (code, (pub, html_list)) in scraped_results:
            all_records = []
            for chunk in html_list:
                recs = self._parse_stock_table(chunk)
                all_records.extend(recs)
            parsed_dict[code] = all_records
            today_str = datetime.now().strftime('%d.%m.%Y')
            last_dates[code] = today_str

        with open(self.LAST_DATES_JSON, 'w') as jf:
            json.dump(last_dates, jf)

        return parsed_dict

    def _parse_stock_table(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'id':'resultsTable'})
        data = []
        if table:
            rows = table.find_all('tr')[1:]
            for row in rows:
                cols = row.find_all('td')
                if len(cols)>=9:
                    data.append({
                        'Date': cols[0].text.strip(),
                        'Price': cols[1].text.strip(),
                        'Max': cols[2].text.strip(),
                        'Min': cols[3].text.strip(),
                        'Avg': cols[4].text.strip(),
                        'Percent Change': cols[5].text.strip(),
                        'Quantity': cols[6].text.strip(),
                        'Best Turnover': cols[7].text.strip(),
                        'Total Turnover': cols[8].text.strip()
                    })
        return data

    def save_data(self, parsed_dict):
        conn = sqlite3.connect(self.STOCK_DB)
        c = conn.cursor()
        for pub_code, recs in parsed_dict.items():
            for r in recs:
                c.execute("""
                    INSERT OR REPLACE INTO stock_data (
                        publisher_code, date, price, max, min, avg,
                        percent_change, quantity, best_turnover, total_turnover
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pub_code,
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
        conn.commit()
        conn.close()
        print("Filter2: Inserted new data (no deletion).")

    def call_next_filter(self):
        print("Filter2: Calling Filter3 now...")
        from filter3 import Filter3
        f3 = Filter3()
        f3.run()

def main():
    f2 = Filter2()
    f2.run()

if __name__ == "__main__":
    main()
