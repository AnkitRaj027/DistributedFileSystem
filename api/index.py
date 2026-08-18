# api/index.py
import sys
import os

# Adds parent directory to sys.path to allow imports from the root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the Flask application instance from app.py
from app import app
