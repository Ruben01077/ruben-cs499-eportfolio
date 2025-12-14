"""
CRUD_Python_Module.py
Enhanced version for CS 499 Milestone Four – Databases

This module defines the AnimalShelter class, which provides a small data-access
layer around a MongoDB collection using the CRUD pattern.

Milestone Four database-focused enhancements include:
- Optional use of environment variables for MongoDB credentials
- Cleaning MongoDB result documents to remove internal _id fields
- Clearer documentation of database behaviors and failure modes
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
import logging

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError


# ----------------------------------------------------------------------
# Basic logger configuration
# ----------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class AnimalShelter:
    """Data access object (DAO) for the AAC 'animals' collection.

    This class provides a small abstraction layer for interacting with
    a MongoDB collection that stores animal records.

    Parameters
    ----------
    user : str
        MongoDB username with access to the target database.
        If the environment variable AAC_DB_USER is set, it will override this value.
    password : str
        Password for the MongoDB user.
        If the environment variable AAC_DB_PASS is set, it will override this value.
    host : str, optional
        Hostname or IP address of the MongoDB server (e.g., "localhost").
    port : int, optional
        Port number of the MongoDB server (e.g., 27017).
    db : str, optional
        Name of the MongoDB database (e.g., "aac").
    col : str, optional
        Name of the collection containing animal records (e.g., "animals").
    authSource : str, optional
        Authentication database; defaults to "admin".
    """

    def __init__(
        self,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 27017,
        db: str = "aac",
        col: str = "animals",
        authSource: str = "admin",
    ) -> None:
        # Allow secure override from environment variables if provided
        env_user = os.getenv("AAC_DB_USER")
        env_pass = os.getenv("AAC_DB_PASS")
        if env_user:
            user = env_user
        if env_pass:
            password = env_pass

        if not user or not password:
            raise ValueError("MongoDB user and password must be provided")

        self._client: Optional[MongoClient] = None
        self._db_name = db
        self._col_name = col

        try:
            logger.info("Connecting to MongoDB at %s:%s", host, port)
            self._client = MongoClient(
                host=host,
                port=port,
                username=user,
                password=password,
                authSource=authSource,
                serverSelectionTimeoutMS=5000,
            )

            # Connectivity check
            self._client.admin.command("ping")
            logger.info("Successfully connected to MongoDB")

        except (ServerSelectionTimeoutError, PyMongoError) as exc:
            logger.error("Failed to connect to MongoDB: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @property
    def collection(self) -> Collection:
        """Return the underlying MongoDB collection object."""
        if self._client is None:
            raise RuntimeError("MongoDB client is not initialized")
        db = self._client[self._db_name]
        return db[self._col_name]

    @staticmethod
    def _clean_results(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Return a copy of documents with internal MongoDB fields removed.

        Currently:
        - Removes the '_id' field, which is an implementation detail.
        """
        cleaned: List[Dict[str, Any]] = []
        for doc in docs:
            d = dict(doc)
            d.pop("_id", None)
            cleaned.append(d)
        return cleaned

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------
    def create(self, data: Dict[str, Any]) -> bool:
        """Insert a single document.

        Returns True if the insert is acknowledged and an _id was created.

        Raises
        ------
        ValueError
            If `data` is empty or not a dictionary.
        PyMongoError
            If the underlying insert operation fails.
        """
        if not isinstance(data, dict) or not data:
            raise ValueError("create() expects a non-empty dict")

        try:
            result = self.collection.insert_one(data)
            success = bool(result.acknowledged and result.inserted_id)
            logger.info(
                "Inserted document with _id=%s (success=%s)",
                result.inserted_id,
                success,
            )
            return success
        except PyMongoError as exc:
            logger.error("Error inserting document: %s", exc)
            raise

    def read(
        self,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: int = 0,
        sort: Optional[List[Tuple[str, int]]] = None,
    ) -> List[Dict[str, Any]]:
        """Read documents matching a query.

        Parameters
        ----------
        query : dict, optional
            A MongoDB query document. If None or empty, all documents are returned.
        projection : dict, optional
            Optional projection document to limit returned fields.
        limit : int, optional
            Maximum number of documents to return. 0 means no explicit limit.
        sort : list of (str, int), optional
            Optional list of (field, direction) pairs for sorting.

        Returns
        -------
        list of dict
            A list of matching documents with MongoDB-internal fields removed.
        """
        if query is None:
            query = {}

        if not isinstance(query, dict):
            raise ValueError("read() expects query to be a dict or None")

        try:
            cursor = self.collection.find(query, projection)
            if sort:
                cursor = cursor.sort(sort)
            if isinstance(limit, int) and limit > 0:
                cursor = cursor.limit(limit)

            raw_results = list(cursor)
            results = self._clean_results(raw_results)
            logger.info("Read %d document(s) from collection", len(results))
            return results
        except PyMongoError as exc:
            logger.error("Error reading documents: %s", exc)
            raise

    def update(
        self,
        query: Dict[str, Any],
        update_doc: Dict[str, Any],
        many: bool = False,
        upsert: bool = False,
    ) -> int:
        """Update one or many documents matching `query`.

        Returns the number of modified documents.

        Raises
        ------
        ValueError
            If `query` or `update_doc` is invalid.
        PyMongoError
            If the underlying update operation fails.
        """
        if not isinstance(query, dict) or not query:
            raise ValueError("update() expects a non-empty query dict")
        if not isinstance(update_doc, dict) or not update_doc:
            raise ValueError("update() expects a non-empty update_doc dict")

        try:
            if many:
                result = self.collection.update_many(query, update_doc, upsert=upsert)
            else:
                result = self.collection.update_one(query, update_doc, upsert=upsert)

            modified_count = int(result.modified_count or 0)
            logger.info("Updated %d document(s)", modified_count)
            return modified_count
        except PyMongoError as exc:
            logger.error("Error updating document(s): %s", exc)
            raise

    def delete(self, query: Dict[str, Any], many: bool = False) -> int:
        """Delete one or many documents matching `query`.

        Returns the number of deleted documents.

        Raises
        ------
        ValueError
            If `query` is invalid.
        PyMongoError
            If the underlying delete operation fails.
        """
        if not isinstance(query, dict) or not query:
            raise ValueError("delete() expects a non-empty query dict")

        try:
            if many:
                result = self.collection.delete_many(query)
            else:
                result = self.collection.delete_one(query)

            deleted_count = int(result.deleted_count or 0)
            logger.info("Deleted %d document(s)", deleted_count)
            return deleted_count
        except PyMongoError as exc:
            logger.error("Error deleting document(s): %s", exc)
            raise

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close the underlying MongoDB client connection, if open."""
        if self._client is not None:
            logger.info("Closing MongoDB client connection")
            self._client.close()
            self._client = None

    def __repr__(self) -> str:
        return f"<AnimalShelter db='{self._db_name}' col='{self._col_name}'>"
