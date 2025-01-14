import os
import sqlite3
from flask import Flask, jsonify, request
from flask_cors import CORS
from pathlib import Path
from datetime import datetime

# IMPORTANT: Keep the import as you originally had it. If your technical_analysis.py
# is in the same folder, or a sibling folder, adjust accordingly.
# If your original code was just "from technical_analysis import compute_all_indicators_and_aggregate",
# we'll preserve that:
from technical_analysis import compute_all_indicators_and_aggregate

app = Flask(__name__)
CORS(app)

PUBLISHERS_DB_PATH = Path(__file__).parent / "publishers.db"
STOCK_DB_PATH = Path(__file__).parent / "stock_data.db"

def init_db():
    """
    Creates initial tables if they do not exist. Called once on startup.
    """
    conn = sqlite3.connect(PUBLISHERS_DB_PATH)
    cursor = conn.cursor()

    # Create publishers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS publishers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publisher_code TEXT UNIQUE
        )
    """)
    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            message TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def query_db(db_path, query, params=()):
    """
    Helper for SELECT queries. Returns a list of rows (tuples).
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"DB SELECT Error: {e}")
        return []

def execute_db(db_path, query, params=()):
    """
    Helper for INSERT/UPDATE/DELETE queries. Commits changes.
    Returns True if successful, False otherwise.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"DB EXEC Error: {e}")
        return False

@app.route("/api/publishers", methods=["GET"])
def get_publishers():
    """
    GET /api/publishers
    Fetch all publisher codes from the publishers database.
    """
    try:
        rows = query_db(PUBLISHERS_DB_PATH, "SELECT publisher_code FROM publishers")
        pubs = [row[0] for row in rows]
        return jsonify({"publishers": pubs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/stock_data", methods=["GET"])
def get_stock_data():
    """
    GET /api/stock_data?publisher=...
    Fetches stock records for the specified publisher.
    """
    publisher = request.args.get("publisher", "").strip()
    if not publisher:
        return jsonify({"error": "Missing 'publisher' query param"}), 400

    try:
        query = """
            SELECT date, price, quantity, max, min, avg, percent_change, total_turnover
            FROM stock_data
            WHERE publisher_code = ?
            ORDER BY date ASC
        """
        rows = query_db(STOCK_DB_PATH, query, (publisher,))
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

@app.route("/api/users", methods=["POST"])
def create_user():
    """
    POST /api/users
    Expects JSON: { name, email, message }
    Inserts a new user record into publishers.db.
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    name = data.get("name", "").strip()
    email = data.get("email", "").strip()
    message = data.get("message", "").strip()

    if not name or not email or not message:
        return jsonify({"error": "Missing name/email/message"}), 400

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = """
        INSERT INTO users (name, email, message, created_at)
        VALUES (?, ?, ?, ?)
    """
    success = execute_db(PUBLISHERS_DB_PATH, query, (name, email, message, now_str))
    if success:
        return jsonify({"status": "ok", "msg": "User info saved"}), 200
    else:
        return jsonify({"error": "Failed to insert user"}), 500

@app.route("/api/technical_analysis", methods=["GET"])
def get_technical_analysis():
    """
    GET /api/technical_analysis?publisher=INTP&tf=1D
    Calls compute_all_indicators_and_aggregate for the given publisher and timeframe.
    """
    publisher = request.args.get("publisher", "").strip()
    tf = request.args.get("tf", "1D").strip()
    if not publisher:
        return jsonify({"error": "Missing 'publisher' query param"}), 400

    try:
        result = compute_all_indicators_and_aggregate(publisher, tf)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000)
