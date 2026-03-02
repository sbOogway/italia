# writes all data to the mongodb 
#
# early attempt of cleaning them, should be put more effort

import json
import os
import re
import traceback
from pathlib import Path
from pprint import pprint

import pandas as pd
from __init__ import entity_file, logger
from dotenv import load_dotenv
from geopy.point import Point
from pymongo import MongoClient

base = Path(__file__).parent / "data" / "raw"
load_dotenv(base.parent.parent / ".env")

client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGODB_DB", "italia")]

italy = base / "Italia" / entity_file
regioni = list((base / "Italia").glob("*/entity.csv"))
province = list((base / "Italia").glob("*/*/entity.csv"))
comuni = list((base / "Italia").glob("*/*/[!entity]*.csv"))
comuni_val_daosta = list((base / "Italia").glob("Valle_d'Aosta/[!entity]*.csv"))

comuni.extend(comuni_val_daosta)

print(len(regioni))
print(len(province))
print(len(comuni))


def coords_to_dd(coords_str):
    p = Point(coords_str)
    
    return {
        "type": "Point",
        "coordinates": [p.latitude, p.longitude]
    }

def json_dump(file, collection, **kwargs):
    df = pd.read_csv(file, header=None)
    df = df[df[0] != df[1]]
    df.loc[0] = ["Nome", df.iloc[0, 0].split(" comune")[0]]

    df = df.dropna()
    obj = {}
    for k, v in zip(df[0], df[1]):
        obj[k] = v

    try:
        abitanti = obj["Abitanti"]
    except KeyError:
        pass
    else:
        numero_pulito = re.sub(r'[^\d]', '', abitanti.split('[')[0])

        data_estratta = re.search(r'\((.*?)\)', abitanti).group(1)

        # more data cleaning should be done here
        obj["Abitanti"] = int(numero_pulito)
        obj["Abitanti data"] = data_estratta

        if kwargs.get("coordinate_geojson", False):
            try:
                obj["Posizione"] = coords_to_dd(obj["Coordinate"])
            except KeyError:
                obj["Posizione"] = coords_to_dd(obj["Coordinate del capoluogo"])

    collection.insert_one(obj)
    pprint(obj)

json_dump(italy, db["stato"])
for regione in regioni:
    json_dump(regione, db["regioni"])

for provincia in province:
    json_dump(provincia, db["province"])

for comune in comuni:
    json_dump(comune, db["comuni"], coordinate_geojson=True)

logger.info("dump to db complete")
