# Code that handles the data fetching from the source API
# Yet to write a complete script that handles that.
from datetime import datetime, timedelta
import os
import requests
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("")
if not API_KEY:
    raise RuntimeError("API Key is not configured in airflow environment!")

BASE_API_URL = ""

def fetch_flight_data():
    