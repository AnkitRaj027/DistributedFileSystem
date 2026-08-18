# api/index.py
import sys
import os
import traceback

# Adds parent directory to sys.path to allow imports from the root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    # Import the Flask application instance from app.py
    from app import app
except Exception as e:
    # Fallback Flask app to expose the exact traceback in case of a startup crash
    from flask import Flask, jsonify
    app = Flask(__name__)

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
    def catch_all(path):
        tb = traceback.format_exc()
        print(f"[BOOTSTRAP ERROR]\n{tb}", file=sys.stderr)
        return jsonify({
            "error": "Bootstrap failed",
            "exception": str(e),
            "traceback": tb.split("\n")
        }), 500
