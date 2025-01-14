"""
filter2.py

Checks the last date of data for each publisher, scrapes missing data
from MSE site, and saves to stock_data.db. Then calls Filter3,
generating last_dates.json so Filter3 knows what to do.
"""

import requests
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from Homework1.filters.base_filter import BaseFilter

class Filter2(BaseFilter):
    """
    Filter2:
      1) Reads all publisher codes from publishers.db
      2) For each publisher, determines last stored date
      3) Scrapes missing data from the MSE site
      4) Saves new records in stock_data.db
      5) Writes last_dates.json for Filter3
      6) Calls Filter3
    """

    def __init__(self):
        super().__init__()
        self.THIS_FOLDER = Path(__file__).parent.resolve()
        self.HOMEWORK3_PATH = self.THIS_FOLDER.parent.parent / "Homework3"
        self.PUBLISHERS_DB = self.HOMEWORK3_PATH / "publishers.db"
        self.STOCK_DB = self.HOMEWORK3_PATH / "stock_data.db"

        self.BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'
        self.LAST_DATES_JSON = self.THIS_FOLDER / "last_dates.json"

    def setup(self):
        """
        Optional hook. Could do any concurrency or path checks here.
        """
        print("Filter2 setup: Preparing to fetch missing data for each publisher...")

    def scrape_data(self):
        """
        Scrape data for ALL publishers concurrently. Return a list of
        (publisher_code, (publisher_code, [list_of_html])).
        """
        with sqlite3.connect(self.PUBLISHERS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT publisher_code FROM publishers")
            publisher_codes = [row[0] for row in cursor.fetchall()]

        if not publisher_codes:
            print("Filter2: No publisher codes found in publishers.db.")
            return []

        results = []

        def fetch_for_publisher(code):
            return (code, self._fetch_publisher_data(code))

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_code = {
                executor.submit(fetch_for_publisher, code): code
                for code in publisher_codes
            }
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    code_result = future.result()
                    results.append(code_result)
                except Exception as e:
                    print(f"Filter2: Exception for publisher {code}: {e}")

        return results

    def _fetch_publisher_data(self, publisher_code):
        last_date_in_db = self._get_last_data_date(publisher_code)
        if last_date_in_db:
            from_dt = datetime.strptime(last_date_in_db, '%d.%m.%Y') + timedelta(days=1)
            print(f"Filter2: Publisher {publisher_code} last data: {last_date_in_db}, fetching missing data.")
        else:
            from_dt = datetime.now() - timedelta(days=365 * 10)
            print(f"Filter2: Publisher {publisher_code} has no data, fetching 10 years of data.")

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
                print(f"Filter2: Failed to fetch data for {publisher_code} from {from_dt} to {end_date}")

            from_dt = end_date + timedelta(days=1)

        return (publisher_code, combined_html)

    def _get_last_data_date(self, publisher_code):
        conn = sqlite3.connect(self.STOCK_DB)
        cursor = conn.cursor()
        cursor.execute('''
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
        ''')
        cursor.execute(
            "SELECT MAX(date) FROM stock_data WHERE publisher_code = ?",
            (publisher_code,)
        )
        last_date = cursor.fetchone()[0]
        conn.close()
        return last_date

    def parse_data(self, scraped_results):
        """
        Convert raw HTML lists to structured row data for each publisher.
        Also build a last_dates dict so Filter3 can see up to which date
        Filter2 has fetched data. We'll just set it to today's date for
        each code to replicate original logic.
        """
        parsed_dict = {}
        last_dates = {}  # track each publisher's "last date fetched"

        for (code, (pub_code, html_list)) in scraped_results:
            all_records = []
            for html_chunk in html_list:
                records = self._parse_stock_table(html_chunk)
                all_records.extend(records)

            parsed_dict[code] = all_records

            # Typically, you'd store the actual max date from the records, but
            # to emulate original logic, we just set "today" as the last date.
            today_str = datetime.now().strftime('%d.%m.%Y')
            last_dates[code] = today_str

        # Write last_dates to JSON file, so Filter3 knows what's new
        with open(self.LAST_DATES_JSON, 'w') as json_file:
            json.dump(last_dates, json_file)

        return parsed_dict

    def _parse_stock_table(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'id': 'resultsTable'})
        data = []
        if table:
            rows = table.find_all('tr')[1:]
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 9:
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
        """
        Insert or replace each row into stock_data.db
        """
        conn = sqlite3.connect(self.STOCK_DB)
        cursor = conn.cursor()
        for pub_code, records in parsed_dict.items():
            for record in records:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_data (
                        publisher_code, date, price, max, min, avg,
                        percent_change, quantity, best_turnover, total_turnover
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    pub_code,
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
        conn.commit()
        conn.close()
        print("Filter2: Saved scraped data to stock_data.db.")

    def call_next_filter(self):
        """
        After saving data, call Filter3 to fill in missing or final data.
        """
        print("Filter2: Calling Filter3 next...")
        from Homework1.filters.filter3 import Filter3
        f3 = Filter3()
        f3.run()


def main():
    f2 = Filter2()
    f2.run()

if __name__ == '__main__':
    main()
