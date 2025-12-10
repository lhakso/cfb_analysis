import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CFBD_API_KEY")

BASE_URL = "https://api.collegefootballdata.com"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
}


def cfbd_get(path: str, params: dict | None = None):
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, headers=HEADERS, params=params or {})
    resp.raise_for_status()
    return resp.json()
