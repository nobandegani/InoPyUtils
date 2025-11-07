#!/usr/bin/env python3
"""
MongoHelper usage example (opt-in demo).

How to run (requires a MongoDB server and the 'motor' + 'pymongo' packages):
- Set environment variables (or rely on defaults):
    - RUN_MONGO_EXAMPLE=1                 # required to actually run the example
    - MONGO_URI=mongodb://localhost:27017 # optional
    - MONGO_DB=inopyutils_demo            # optional
    - MONGO_COLLECTION=demo_items         # optional
- Then execute this file directly with Python:
    python tests/mongo_helper/test_mongo_helper_example.py

Notes:
- Import of mongo_helper is intentionally done inside the main() function so simply
  importing this module won't require motor/pymongo unless you actually run it.
- This is a simple end-to-end flow: connect → insert → get → find → update → aggregate
  → count → delete → close. It prints ok/err shaped dicts so you can see results.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict


async def main() -> None:
    # Opt-in guard so this example doesn't run during automated test runs
    if os.environ.get("RUN_MONGO_EXAMPLE", "1") != "1":
        print("[MongoHelper Example] Skipped. Set RUN_MONGO_EXAMPLE=1 to run this demo.")
        return

    # Lazy import to avoid requiring motor/pymongo at import time of this file
    from src.inopyutils.mongo_helper import mongo

    uri = os.environ.get("MONGO_URI")
    db_name = os.environ.get("MONGO_DB", "inopyutils_demo")
    collection = os.environ.get("MONGO_COLLECTION", "demo_items")

    host = os.environ.get("MONGO_HOST", "localhost")
    port_env = os.environ.get("MONGO_PORT", "27017")
    username = ""
    password = ""
    auth_source = os.environ.get("MONGO_AUTH_SOURCE")
    port = int(port_env) if port_env and port_env.isdigit() else None

    print("=== MongoHelper Example ===")
    if uri:
        print(f"Connecting via URI: {uri}, db={db_name} ...")
    else:
        print(f"Connecting via components: host={host or 'localhost'}, port={port or 27017}, user={username!r}, db={db_name} ...")

    # Connect (fail fast if server unreachable). Also ensure DB exists.
    await mongo.connect(
        uri=uri,
        db_name=db_name,
        host=host,
        port=port,
        username=username,
        password=password,
        auth_source=auth_source,
        appname="InoPyUtilsExample",
        check_connection=True,
        ensure_db_exists=True,
        ensure_collection_name="_meta",
        serverSelectionTimeoutMS=5_000,
    )

    # Optionally use context manager to ensure close() is called automatically.
    async with mongo:
        # 1) Insert a document
        doc: Dict[str, Any] = {"name": "Alice", "age": 30, "tags": ["example", "demo"]}
        inserted = await mongo.insert_one(collection, doc)
        print("insert_one =>", inserted)
        if not inserted.get("success") or not inserted.get("inserted_id"):
            print("Insert failed or no inserted_id; aborting demo.")
            return

        inserted_id = inserted["inserted_id"]

        # 2) Get by id (auto-converts string _id to ObjectId in filter)
        got = await mongo.get_by_id(collection, inserted_id)
        print("get_by_id =>", got)

        # 3) Find many using operator filter on _id (e.g., $in)
        #    This demonstrates the helper's normalization for operator-style filters.
        many = await mongo.find_many(collection, {"_id": {"$in": [inserted_id]}})
        print("find_many ($in by _id) =>", many)

        # 4) Update by id
        updated = await mongo.update_by_id(collection, inserted_id, {"$set": {"age": 31}})
        print("update_by_id =>", updated)

        # 5) Aggregate (group by age and count)
        pipeline = [
            {"$match": {}},
            {"$group": {"_id": "$age", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
        agg = await mongo.aggregate(collection, pipeline)
        print("aggregate =>", agg)

        # 6) Count documents
        cnt = await mongo.count_documents(collection)
        print("count_documents =>", cnt)

        # 7) Delete the inserted document
        deleted = await mongo.delete_one(collection, {"_id": inserted_id})
        print("delete_one =>", deleted)

    print("Closed connection. Example finished.")


if __name__ == "__main__":
    asyncio.run(main())
