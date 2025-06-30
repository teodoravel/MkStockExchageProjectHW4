# Homework4/filter_service/filter_service_app.py
"""
filter_service_app.py
Flask microservice to run Filter1, Filter2, Filter3 on demand.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS

from filter1 import Filter1
from filter2 import Filter2
from filter3 import Filter3

app = Flask(__name__)
CORS(app)


@app.route("/filter1", methods=["POST"])
def run_filter1():
    f1 = Filter1()
    f1.run()
    return jsonify({"status": "Filter1 completed"}), 200


@app.route("/filter2", methods=["POST"])
def run_filter2():
    f2 = Filter2()
    f2.run()
    return jsonify({"status": "Filter2 completed"}), 200


@app.route("/filter3", methods=["POST"])
def run_filter3():
    f3 = Filter3()
    f3.run()
    return jsonify({"status": "Filter3 completed"}), 200


# ── new: simple health endpoint ──
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    # bind on all interfaces so the port is reachable from outside the container
    app.run(host="0.0.0.0", port=5001, debug=True)
