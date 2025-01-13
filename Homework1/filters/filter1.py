import requests
from bs4 import BeautifulSoup
import sqlite3
from pathlib import Path
import subprocess

def fetch_publisher_codes():
    url = 'https://www.mse.mk/mk/stats/symbolhistory/avk'
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to fetch issuers.")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    dropdown = soup.find('select', {'id': 'Code'})
    publisher_codes = [
        option.get('value') for option in dropdown.find_all('option')
        if option.get('value') and option.get('value').isalpha()
    ] if dropdown else []
    return publisher_codes

def save_to_database(publishers):
    THIS_FOLDER = Path(__file__).parent.resolve()
    # Go UP two levels to the project root, then down into Homework2/tech_prototype
    TECH_PROTOTYPE_PATH = THIS_FOLDER.parent.parent / "Homework2" / "tech_prototype"
    db_path = TECH_PROTOTYPE_PATH / "publishers.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS publishers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publisher_code TEXT UNIQUE
        )
    ''')
    cursor.execute("DELETE FROM publishers")  # We want a fresh start each time
    for publisher_code in publishers:
        cursor.execute(
            "INSERT OR IGNORE INTO publishers (publisher_code) VALUES (?)",
            (publisher_code,)
        )
    conn.commit()
    conn.close()

def call_filter2():
    filter2_path = Path(__file__).parent / "filter2.py"
    subprocess.run(["python", str(filter2_path)])

def main():
    publisher_codes = fetch_publisher_codes()
    if publisher_codes:
        print(f"Found {len(publisher_codes)} issuers.")
        unique_codes = list(set(publisher_codes))
        save_to_database(unique_codes)
        print("Filter1 completed. Calling Filter2...")
        call_filter2()
    else:
        print("No issuers found.")

if __name__ == '__main__':
    main()
