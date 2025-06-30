# Homework4/analysis_service/analysis_service_app.py
"""
analysis_service_app.py
A Flask microservice that uses technical_analysis.py
to compute indicators for a given publisher & timeframe.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from technical_analysis import compute_all_indicators_and_aggregate

app = Flask(__name__)
CORS(app)


@app.route("/analysis", methods=["GET"])
def do_analysis():
    publisher = request.args.get("publisher", "").strip()
    tf = request.args.get("tf", "1D").strip()
    if not publisher:
        return jsonify({"error": "Missing 'publisher'"}), 400

    try:
        result = compute_all_indicators_and_aggregate(publisher, tf)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── new: simple health endpoint ──
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    # bind on all interfaces so the port is reachable from outside the container
    app.run(host="0.0.0.0", port=5000, debug=True)
