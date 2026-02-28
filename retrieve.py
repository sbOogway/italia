import os
from pathlib import Path

import pandas as pd
from bson import json_util
from dotenv import load_dotenv
from pymongo import MongoClient

base = Path(__file__).parent / "data"
load_dotenv(base.parent / ".env")


json_dir = base / "clean" / "json"
csv_dir = base / "clean" / "csv"

json_dir.mkdir(parents=True, exist_ok=True)
csv_dir.mkdir(parents=True, exist_ok=True)

collezioni = ["stato", "regioni", "province", "comuni"]
client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGODB_DB", "italia")]


for collezione in collezioni:
    collection = db[collezione]
    cursor = collection.find()
    data = list(cursor)

    with open(json_dir / f"{collezione}.json", "w") as f:
        f.write(json_util.dumps(data, indent=4))

    df = pd.DataFrame(data)

    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)

    df.to_csv(csv_dir / f"{collezione}.csv", index=False)

