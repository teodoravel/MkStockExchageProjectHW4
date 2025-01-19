# Homework4/gateway/app.py

"""
app.py - Gateway on port 5000.
Now we do NOT automatically call Filter2 and Filter3 on each technical analysis request.
"""

import requests
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS
from pathlib import Path
from datetime import datetime

app = Flask(__name__)
CORS(app)

THIS_FOLDER = Path(__file__).parent.parent
PUBLISHERS_DB = THIS_FOLDER / "publishers.db"
STOCK_DB = THIS_FOLDER / "stock_data.db"

@app.route("/api/publishers", methods=["GET"])
def get_publishers():
    try:
        conn = sqlite3.connect(PUBLISHERS_DB)
        c = conn.cursor()
        c.execute("SELECT publisher_code FROM publishers")
        rows = c.fetchall()
        conn.close()
        pubs = [r[0] for r in rows]
        return jsonify({"publishers": pubs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/stock_data", methods=["GET"])
def get_stock_data():
    publisher = request.args.get("publisher", "").strip()
    if not publisher:
        return jsonify({"error": "Missing publisher"}), 400

    # an on-demand refresh:
    # requests.post("http://localhost:5001/filter2")
    # requests.post("http://localhost:5001/filter3")

    try:
        conn = sqlite3.connect(STOCK_DB)
        c = conn.cursor()
        c.execute("""
            SELECT date, price, quantity, max, min, avg, percent_change, total_turnover
            FROM stock_data
            WHERE publisher_code = ?
            ORDER BY date ASC
        """, (publisher,))
        rows = c.fetchall()
        conn.close()

        data_list = []
        for row in rows:
            data_list.append({
                "date": row[0],
                "price": row[1],
                "volume": row[2],
                "max": row[3],
                "min": row[4],
                "avg": row[5],
                "percent_change": row[6],
                "total_turnover": row[7]
            })
        return jsonify({"publisher": publisher, "records": data_list}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/technical_analysis", methods=["GET"])
def get_technical_analysis():
    """
    We comment out the lines that re-run filter2/3 automatically:
    # requests.post("http://localhost:5001/filter2")
    # requests.post("http://localhost:5001/filter3")
    Then just call analysis microservice.
    """
    publisher = request.args.get("publisher","").strip()
    tf = request.args.get("tf","1D").strip()
    if not publisher:
        return jsonify({"error": "Missing 'publisher'"}), 400

    # REMOVED auto-scraping:
    # requests.post("http://localhost:5001/filter2")
    # requests.post("http://localhost:5001/filter3")

    try:
        # only call analysis microservice
        analysis_url = f"http://localhost:5002/analysis?publisher={publisher}&tf={tf}"
        r = requests.get(analysis_url)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/run_all_filters", methods=["POST"])
def run_all_filters():
    """
    Single endpoint to run Filter1->Filter2->Filter3 if you want.
    """
    requests.post("http://localhost:5001/filter1")
    requests.post("http://localhost:5001/filter2")
    requests.post("http://localhost:5001/filter3")
    return jsonify({"status":"All filters completed"}), 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)
