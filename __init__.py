import logging
import uuid
from pathlib import Path

from curl_cffi import requests


def get_ninja_scraper() -> requests.Session:
    return requests.Session(impersonate="chrome120")

entity_file = "entity.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "logs" / f"{uuid.uuid7()}.log" ), # Salva su file
        logging.StreamHandler()  # Stampa a video
    ],
)

logger = logging.getLogger("logger")