"""
Async MongoDB helper for FastAPI-style projects.

Design goals
- Lazy optional dependency: importing this module must NOT require motor or pymongo.
- Single reusable instance: initialize once on app startup, import and use everywhere.
- Fully typed public API (without importing motor at runtime) with helpful docstrings.
- Thin wrappers around common MongoDB operations that return plain dicts and str IDs.

Example (FastAPI)

    from fastapi import FastAPI
    from inopyutils.mongo_helper import mongo

    app = FastAPI()

    @app.on_event("startup")
    async def on_startup() -> None:
        await mongo.connect(
            uri="mongodb://localhost:27017",
            db_name="mydb",
            serverSelectionTimeoutMS=5_000,
            check_connection=True,           # ping on startup (default True)
            ensure_db_exists=True,           # materialize DB if empty (optional)
            ensure_collection_name="_meta", # name of tiny collection used to materialize
        )

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await mongo.close()

    # Usage in routes or services
    # document = await mongo.find_one("users", {"_id": user_id})

Notes
- Methods raise NotInitializedError if used before connect().
- motor (async driver) is only imported inside methods that need it.
- By default, ObjectId values are converted to str in returned documents.
"""
from __future__ import annotations

from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    Literal,
)
import asyncio
from datetime import datetime, timezone
from urllib.parse import quote_plus
from .util_helper import ok, err
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

# Type aliases that do not require motor at runtime
Document = Dict[str, Any]
Filter = Mapping[str, Any]
Projection = Optional[Mapping[str, Union[int, bool]]]
SortDirection = Literal[1, -1]
Sort = Sequence[Tuple[str, SortDirection]]
Pipeline = Sequence[Mapping[str, Any]]



class NotInitializedError(RuntimeError):
    """Raised when MongoHelper is used before connect()."""


class MongoHelper:
    """Async MongoDB helper wrapping motor for common operations.

    This helper hides low-level driver details and exposes a simple, typed API.
    It is safe to import anywhere and initialize once at application startup.

    ObjectId handling
    - By default, returned documents convert the top-level `_id` field to str.
    - Filters that contain `_id` as str are automatically converted to ObjectId.
    - You can override conversion per-call using `convert_id_to_str`.
    """

    def __init__(self, *, convert_id_to_str: bool = True) -> None:
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None
        self._db_name: Optional[str] = None
        self._uri: Optional[str] = None
        self._convert_id_to_str_default: bool = convert_id_to_str
        self._lock = asyncio.Lock()

    # ------------------------------
    # Connection management
    # ------------------------------
    @property
    def is_connected(self) -> bool:
        return self._db is not None and self._client is not None

    @property
    def db_name(self) -> Optional[str]:
        return self._db_name

    async def connect(
        self,
        *,
        uri: Optional[str] = None,
        db_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_source: Optional[str] = None,
        appname: Optional[str] = None,
        convert_id_to_str: Optional[bool] = None,
        check_connection: bool = True,
        ensure_db_exists: bool = False,
        ensure_collection_name: Optional[str] = None,
        **client_kwargs: Any,
    ) -> None:
        """Create and (optionally) validate a single shared AsyncIOMotorClient and database.

        Parameters
        - uri: Full MongoDB connection string. If provided, takes precedence over host/port/username/password.
        - db_name: Default database name to use.
        - host: MongoDB host (e.g., 'localhost' or 'my.mongo.internal'). Used if uri is not provided.
        - port: MongoDB port (e.g., 27017). Used if uri is not provided.
        - username: Username for authentication. Used if uri is not provided.
        - password: Password for authentication. Used if uri is not provided.
        - auth_source: Authentication database name (maps to URI param 'authSource') when building from components.
        - appname: Optional appname for MongoDB monitoring/diagnostics.
        - convert_id_to_str: Override instance-wide default for id conversion.
        - check_connection: If True (default), ping the DB on connect to fail fast.
        - ensure_db_exists: If True, materialize the database if empty by creating a small collection.
        - ensure_collection_name: Name of the collection to create when materializing the DB (default: "_meta").
        - client_kwargs: Passed directly to AsyncIOMotorClient (timeouts, SSL, etc.).

        This method is safe to call multiple times; duplicate calls are no-ops
        if already connected with the same parameters.
        """
        if convert_id_to_str is not None:
            self._convert_id_to_str_default = convert_id_to_str

        # Build a URI from components when not provided
        final_uri = uri
        if not final_uri:
            if not host and not port and not username and not password:
                raise ValueError("Either 'uri' or at least 'host' (and optionally port/username/password) must be provided.")
            host_part = host or "localhost"
            port_part = f":{port}" if port else ""
            auth_part = ""
            if username:
                u = quote_plus(username)
                if password is not None:
                    p = quote_plus(password)
                    auth_part = f"{u}:{p}@"
                else:
                    auth_part = f"{u}@"
            final_uri = f"mongodb://{auth_part}{host_part}{port_part}"

            # Attach authSource query if requested and not already present
            if auth_source:
                sep = "&" if "?" in final_uri else "?"
                final_uri = f"{final_uri}{sep}authSource={quote_plus(auth_source)}"

        async with self._lock:
            if self.is_connected:
                # If already connected to the same target, just return
                if self._uri == final_uri and self._db_name == db_name:
                    return
                # Otherwise close the existing connection before re-connecting
                await self.close()

            try:  # Lazy import to keep dependency optional at import time
                import motor.motor_asyncio as motor
            except Exception as e:  # pragma: no cover - environment dependent
                raise RuntimeError(
                    "motor is required to use MongoHelper.connect(). Install with 'pip install motor'."
                ) from e

            if appname and "appname" not in client_kwargs:
                client_kwargs["appname"] = appname

            # Reasonable default to fail fast in startup if server is unreachable
            client_kwargs.setdefault("serverSelectionTimeoutMS", 5_000)

            client = motor.AsyncIOMotorClient(final_uri, **client_kwargs)
            db = client[db_name]

            # Optionally validate connection (forces server selection on startup)
            if check_connection:
                try:
                    await db.command("ping")
                except Exception:
                    # Close the client on failure to avoid leaking sockets
                    try:
                        client.close()
                    finally:
                        pass
                    raise

            # Optionally ensure DB exists/materialized
            if ensure_db_exists:
                await self._ensure_db_exists(db, ensure_collection_name)

            self._client = client
            self._db = db
            self._uri = final_uri
            self._db_name = db_name

    async def _ensure_db_exists(self, db: Any, ensure_collection_name: Optional[str]) -> None:
        """Ensure the database is materialized.

        MongoDB creates a database lazily on first write. If the database has
        no collections yet, optionally create a tiny metadata collection to
        materialize it so subsequent operations (like listing or indexing)
        work consistently.
        """
        try:
            names = await db.list_collection_names()
        except Exception:
            # If the server doesn't support listing or call fails, attempt to
            # materialize anyway and let errors bubble up to caller on failure.
            names = []

        if names:
            return

        coll_name = ensure_collection_name or "_meta"
        try:
            # Create the collection if it doesn't exist (idempotent enough for our purpose)
            await db.create_collection(coll_name)
        except Exception:
            # Ignore if someone else created it concurrently
            pass

        coll = db[coll_name]
        # Upsert a lightweight sentinel document
        try:
            await coll.update_one(
                {"_id": "db_meta"},
                {"$setOnInsert": {"createdAt": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            )
        except Exception:
            # Ignore sentinel failure; DB may still be created
            pass

    async def close(self) -> None:
        """Close the underlying AsyncIOMotorClient, if any."""
        async with self._lock:
            if self._client is not None:
                # motor's close() is sync and immediate
                self._client.close()
            self._client = None
            self._db = None
            self._uri = None
            self._db_name = None

    async def ping(self) -> Dict[str, Any]:
        """Ping the database and return ok/err dict."""
        try:
            db = self._require_db()
            await db.command("ping")
            return ok("pong", connected=True)
        except Exception as e:
            return err("ping_failed", error=str(e))

    # ------------------------------
    # Core operations
    # ------------------------------
    def _require_db(self) -> AsyncIOMotorDatabase:
        if self._db is None:
            raise NotInitializedError(
                "MongoHelper is not initialized. Call await mongo.connect(...) during app startup."
            )
        return self._db

    def _collection(self, name: str) -> AsyncIOMotorCollection:
        return self._require_db()[name]

    @staticmethod
    def _try_import_objectid() -> Any:
        try:
            from bson import ObjectId  # type: ignore
        except Exception as e:  # pragma: no cover - environment dependent
            raise RuntimeError(
                "bson (from pymongo) is required at runtime for ObjectId operations. Install with 'pip install pymongo'."
            ) from e
        return ObjectId

    def _to_object_id(self, value: Any) -> Any:
        ObjectId = self._try_import_objectid()
        if isinstance(value, ObjectId):
            return value
        if isinstance(value, str):
            try:
                return ObjectId(value)
            except Exception:
                return value
        return value

    def _normalize_filter(self, flt: Optional[Filter]) -> Dict[str, Any]:
        if not flt:
            return {}
        # Copy and normalize `_id`
        out: Dict[str, Any] = dict(flt)
        if "_id" in out:
            out_id = out["_id"]
            # Support operator-style filters on _id (e.g., {"_id": {"$in": [...]}})
            if isinstance(out_id, dict):
                op_map: Dict[str, Any] = dict(out_id)
                # List-like operators
                for op in ("$in", "$nin"):
                    if op in op_map and isinstance(op_map[op], (list, tuple, set)):
                        op_map[op] = [self._to_object_id(v) for v in op_map[op]]  # type: ignore[index]
                # Scalar operators
                for op in ("$eq", "$ne"):
                    if op in op_map:
                        op_map[op] = self._to_object_id(op_map[op])  # type: ignore[index]
                out["_id"] = op_map
            elif isinstance(out_id, (str, bytes)) or (hasattr(out_id, "__iter__") and not isinstance(out_id, (dict,))):
                # Handle both scalar and list of IDs
                if isinstance(out_id, (list, tuple, set)):
                    out["_id"] = [self._to_object_id(v) for v in out_id]  # type: ignore[assignment]
                else:
                    out["_id"] = self._to_object_id(out_id)
        return out

    def _convert_id(self, doc: Optional[Mapping[str, Any]], *, convert_id_to_str: bool) -> Optional[Document]:
        if doc is None:
            return None
        d: Document = dict(doc)
        if convert_id_to_str and "_id" in d:
            try:
                ObjectId = self._try_import_objectid()
                if isinstance(d["_id"], ObjectId):
                    d["_id"] = str(d["_id"])  # type: ignore[assignment]
            except Exception:
                # If bson is not available at runtime we simply leave _id as-is
                pass
        return d

    # ---- find ----
    async def find_one(
        self,
        collection: str,
        flt: Optional[Filter] = None,
        *,
        projection: Projection = None,
        convert_id_to_str: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Find a single document and return ok/err dict with data and found flag."""
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            doc = await coll.find_one(flt_n, projection)  # type: ignore[arg-type]
            converted = self._convert_id(
                doc,
                convert_id_to_str=convert_id_to_str if convert_id_to_str is not None else self._convert_id_to_str_default,
            )
            return ok("found" if converted is not None else "not_found", data=converted, found=converted is not None)
        except Exception as e:
            return err("find_one_failed", error=str(e))

    async def find_many(
        self,
        collection: str,
        flt: Optional[Filter] = None,
        *,
        projection: Projection = None,
        sort: Optional[Sort] = None,
        skip: int = 0,
        limit: int = 0,
        convert_id_to_str: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Find multiple documents and return ok/err dict with items and count."""
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            cursor = coll.find(flt_n, projection)  # type: ignore[arg-type]
            if sort:
                cursor = cursor.sort(list(sort))
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            items: List[Document] = []
            do_convert = convert_id_to_str if convert_id_to_str is not None else self._convert_id_to_str_default
            async for doc in cursor:
                items.append(self._convert_id(doc, convert_id_to_str=do_convert) or {})
            return ok("found_many", items=items, count=len(items))
        except Exception as e:
            return err("find_many_failed", error=str(e))

    async def iter_many(
        self,
        collection: str,
        flt: Optional[Filter] = None,
        *,
        projection: Projection = None,
        sort: Optional[Sort] = None,
        skip: int = 0,
        limit: int = 0,
        convert_id_to_str: Optional[bool] = None,
    ) -> AsyncIterator[Document]:
        coll = self._collection(collection)
        flt_n = self._normalize_filter(flt)
        cursor = coll.find(flt_n, projection)  # type: ignore[arg-type]
        if sort:
            cursor = cursor.sort(list(sort))
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        do_convert = convert_id_to_str if convert_id_to_str is not None else self._convert_id_to_str_default
        async for doc in cursor:
            yield self._convert_id(doc, convert_id_to_str=do_convert) or {}

    # ------------------------------
    # Convenience alias helpers
    # ------------------------------
    async def get_by_id(
        self,
        collection: str,
        _id: Any,
        *,
        projection: Projection = None,
        convert_id_to_str: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Get a single document by its `_id` (returns ok/err dict)."""
        return await self.find_one(
            collection,
            {"_id": _id},
            projection=projection,
            convert_id_to_str=convert_id_to_str,
        )

    async def get_one(
        self,
        collection: str,
        flt: Optional[Filter] = None,
        *,
        projection: Projection = None,
        convert_id_to_str: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Get a single document matching the query (alias to `find_one`), returns ok/err dict."""
        return await self.find_one(
            collection,
            flt,
            projection=projection,
            convert_id_to_str=convert_id_to_str,
        )

    async def get_many(
        self,
        collection: str,
        flt: Optional[Filter] = None,
        *,
        projection: Projection = None,
        sort: Optional[Sort] = None,
        skip: int = 0,
        limit: int = 0,
        convert_id_to_str: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Get multiple documents matching the query (alias to `find_many`), returns ok/err dict."""
        return await self.find_many(
            collection,
            flt,
            projection=projection,
            sort=sort,
            skip=skip,
            limit=limit,
            convert_id_to_str=convert_id_to_str,
        )

    async def update_by_id(
        self,
        collection: str,
        _id: Any,
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> Dict[str, Any]:
        """Update a single document by its `_id` (wrapper over `update_one`)."""
        return await self.update_one(collection, {"_id": _id}, update, upsert=upsert)

    # ---- insert ----
    async def insert_one(self, collection: str, document: Mapping[str, Any]) -> Dict[str, Any]:
        """Insert a document and return ok/err dict with inserted_id."""
        try:
            coll = self._collection(collection)
            doc = dict(document)
            result = await coll.insert_one(doc)  # type: ignore[no-any-return]
            inserted_id = getattr(result, "inserted_id", None)
            return ok("inserted_one", inserted_id=str(inserted_id) if inserted_id is not None else None)
        except Exception as e:
            return err("insert_one_failed", error=str(e))

    async def insert_many(self, collection: str, documents: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            docs = [dict(d) for d in documents]
            result = await coll.insert_many(docs)
            ids = getattr(result, "inserted_ids", [])
            str_ids = [str(_id) for _id in ids]
            return ok("inserted_many", inserted_ids=str_ids, count=len(str_ids))
        except Exception as e:
            return err("insert_many_failed", error=str(e))

    # ---- update/replace ----
    async def update_one(
        self,
        collection: str,
        flt: Filter,
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            result = await coll.update_one(flt_n, dict(update), upsert=upsert)
            return ok(
                "updated_one",
                matched_count=getattr(result, "matched_count", 0),
                modified_count=getattr(result, "modified_count", 0),
                upserted_id=(str(result.upserted_id) if getattr(result, "upserted_id", None) is not None else None),
            )
        except Exception as e:
            return err("update_one_failed", error=str(e))

    async def update_many(
        self,
        collection: str,
        flt: Filter,
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            result = await coll.update_many(flt_n, dict(update), upsert=upsert)
            return ok(
                "updated_many",
                matched_count=getattr(result, "matched_count", 0),
                modified_count=getattr(result, "modified_count", 0),
                upserted_id=(str(result.upserted_id) if getattr(result, "upserted_id", None) is not None else None),
            )
        except Exception as e:
            return err("update_many_failed", error=str(e))

    async def replace_one(
        self,
        collection: str,
        flt: Filter,
        replacement: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            result = await coll.replace_one(flt_n, dict(replacement), upsert=upsert)
            return ok(
                "replaced_one",
                matched_count=getattr(result, "matched_count", 0),
                modified_count=getattr(result, "modified_count", 0),
                upserted_id=(str(result.upserted_id) if getattr(result, "upserted_id", None) is not None else None),
            )
        except Exception as e:
            return err("replace_one_failed", error=str(e))

    # ---- delete ----
    async def delete_one(self, collection: str, flt: Filter) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            result = await coll.delete_one(flt_n)
            return ok("deleted_one", deleted_count=getattr(result, "deleted_count", 0))
        except Exception as e:
            return err("delete_one_failed", error=str(e))

    async def delete_many(self, collection: str, flt: Filter) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            result = await coll.delete_many(flt_n)
            return ok("deleted_many", deleted_count=getattr(result, "deleted_count", 0))
        except Exception as e:
            return err("delete_many_failed", error=str(e))

    # ---- aggregate / count / indexes ----
    async def aggregate(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        convert_id_to_str: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Run an aggregation pipeline and return ok/err dict with items and count."""
        try:
            coll = self._collection(collection)
            cursor = coll.aggregate(list(pipeline))
            items: List[Document] = []
            do_convert = convert_id_to_str if convert_id_to_str is not None else self._convert_id_to_str_default
            async for doc in cursor:
                items.append(self._convert_id(doc, convert_id_to_str=do_convert) or {})
            return ok("aggregated", items=items, count=len(items))
        except Exception as e:
            return err("aggregate_failed", error=str(e))

    async def count_documents(self, collection: str, flt: Optional[Filter] = None) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            flt_n = self._normalize_filter(flt)
            cnt = await coll.count_documents(flt_n)
            return ok("counted", count=int(cnt))
        except Exception as e:
            return err("count_documents_failed", error=str(e))

    async def create_index(
        self, collection: str, keys: Sequence[Tuple[str, SortDirection]], *, unique: bool = False, **kwargs: Any
    ) -> Dict[str, Any]:
        try:
            coll = self._collection(collection)
            name = await coll.create_index(list(keys), unique=unique, **kwargs)
            return ok("index_created", name=name)
        except Exception as e:
            return err("create_index_failed", error=str(e))

    # ------------------------------
    # Context manager helpers
    # ------------------------------
    async def __aenter__(self) -> "MongoHelper":
        if not self.is_connected:
            raise NotInitializedError("Call connect() before using MongoHelper as a context manager.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()


# Reusable module-level instance to import across the app
mongo = MongoHelper()

# Backward-compatible alias if some code used the previous class name
MongoClient = MongoHelper
