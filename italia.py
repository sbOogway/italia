import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from pathlib import Path
from threading import Lock

import pandas as pd
from __init__ import entity_file, get_ninja_scraper, logger
from curl_cffi import requests

THREAD_COUNT = 10
file_write_counter = 0
file_write_lock = Lock()

# tor proxies
proxies = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}

table_pattern = r"(<table.*?>.*?<\/table>)"
table_with_index_pattern = r"(<table.*?>.*?<\/table>.*?<\/table>)"
comune_provincia_pattern = r"//it\.wikipedia\.org/wiki/(?!File:)(.*?)\".*?\"//it\.wikipedia\.org/wiki/((?:Provincia_|Città_metropolitana_|Libero_consorzio_comunale_).*?)\""
valle_daosta_link_pattern = r"//it\.wikipedia\.org/wiki/(?!.*(?:Area|Metro|Livello_del_mare|Chilometro_quadrato|Altitudine|Densità_di_popolazione))(.*?)\""
wikipedia_url = "https://it.wikipedia.org/wiki/"


base_path = Path(__file__).parent / "data" / "Italia"


class Regione:
    def __init__(self, nome, preposizione_articolata):
        self.nome = nome
        self.preposizione = preposizione_articolata

    def to_link(self):
        return f"{self.preposizione}{self.nome}"


def _fetch_page(session: requests.Session, url: str) -> str:
    response = session.get(url)
    return response.text


def _parse_tables(html: str, regione: Regione) -> list[str] | None:
    # Campania e Sicilia hanno tabelle con indice
    pattern = (
        table_with_index_pattern
        if regione and regione.nome in {"Campania", "Sicilia"}
        else table_pattern
    )
    if not (tables := re.findall(pattern, html, re.DOTALL)):
        print("No table found")
        return None
    return tables


def _write_territorial_entity(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    logger.info(f"[{file_write_counter}] wrote {path.absolute()} ")

def _create_scraper():
    scraper = get_ninja_scraper()
    scraper.proxies = proxies
    return scraper

def _save_territorial_entity(scraper, path: Path, page_link: str):
    global file_write_counter
    content = get_territorial_entity(scraper, page_link)
    _write_territorial_entity(path, content)
    with file_write_lock:
        file_write_counter += 1
    return path.parent.name

def get_regioni(session: requests.Session):
    url = f"{wikipedia_url}Comuni_d%27Italia"
    page = _fetch_page(session, url)
    tables = _parse_tables(page, None)

    comuni_regione_pattern = r"Comuni_(de.*?)\""

    comuni_regione_links = re.findall(comuni_regione_pattern, tables[0])

    preposizione_nome_pattern = r"(de.*?)([A-Z].*)"

    for regione in comuni_regione_links:
        preposizione, nome = re.findall(preposizione_nome_pattern, regione)[0]

        yield Regione(nome, preposizione)

def get_comuni_in_regione(session: requests.Session, regione: Regione):
    url = f"{wikipedia_url}Comuni_{regione.to_link()}"
    page = _fetch_page(session, url)

    comuni_table = _parse_tables(page, regione)[0]

    # la valle d'aosta non ha province
    pattern = (
        valle_daosta_link_pattern
        if regione.nome == "Valle_d'Aosta"
        else comune_provincia_pattern
    )

    comune_provincia = re.findall(pattern, comuni_table, re.S)

    scraper = _create_scraper()
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        
        if regione.nome != "Valle_d'Aosta":
            args = [
                (
                    scraper,
                    base_path / regione.nome / provincia / f"{comune}.csv",
                    comune,
                )
                for comune, provincia in comune_provincia
            ]
        else:
            args = [
                (
                    scraper,
                    base_path / regione.nome / f"{comune}.csv",
                    comune,
                )
                for comune in comune_provincia
            ]
        province = set(
            executor.map(
                lambda a: _save_territorial_entity(*a) or a[1].parent.name, args
            )
        )

    if regione.nome == "Valle_d'Aosta":
        _save_territorial_entity(
            scraper,
            base_path / "Valle_d'Aosta" / entity_file,
            "Valle_d'Aosta",
        )
        return

    logger.info(f"province trovate: {province}")

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        args = [
            (
                scraper,
                base_path / regione.nome / provincia / entity_file,
                provincia,
            )
            for provincia in province
        ]
        list(executor.map(lambda a: _save_territorial_entity(*a), args))


def get_territorial_entity(session: requests.Session, entity: str):
    url = f"{wikipedia_url}{entity}"
    page = _fetch_page(session, url)
    tables = _parse_tables(page, None)
    df = pd.read_html(StringIO(tables[0]))[0]
    return df.to_csv(index=False)


if __name__ == "__main__":
    scraper = _create_scraper()

    _save_territorial_entity(scraper, base_path / entity_file, "Italia")

    regioni = get_regioni(scraper)

    for regione in regioni:
        _save_territorial_entity(scraper, base_path / regione.nome / entity_file, regione.nome)
        comuni = get_comuni_in_regione(scraper, regione)
