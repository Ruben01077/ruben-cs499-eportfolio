# CRUD_Python_Module.py

from typing import Any, Dict, List, Optional, Sequence, Tuple
from pymongo import MongoClient
from pymongo.errors import PyMongoError

class AnimalShelter(object):
   

    def __init__(
        self,
        user: str = "aacuser",
        password: str = "SNHU1234",
        host: str = "localhost",
        port: int = 27017,
        db: str = "aac",
        col: str = "animals",
        auth_source: str = "admin",
        auth_mechanism: str = "SCRAM-SHA-256",
        server_selection_timeout_ms: int = 5000,
    ) -> None:
        if not user or not password:
            raise ValueError("Both 'user' and 'password' are required.")

        self._client = MongoClient(
            host=host,
            port=port,
            username=user,
            password=password,
            authSource=auth_source,
            authMechanism=auth_mechanism,
            directConnection=True,
            serverSelectionTimeoutMS=server_selection_timeout_ms,
        )

        # Connectivity check
        self._client.admin.command("ping")

        self.database = self._client[db]
        self.collection = self.database[col]

    # C - Create
    def create(self, data: Dict[str, Any]) -> bool:
        if not isinstance(data, dict) or not data:
            raise ValueError("'data' must be a non-empty dict.")
        try:
            res = self.collection.insert_one(data)
            return bool(res.acknowledged and res.inserted_id)
        except PyMongoError as e:
            print(f"[ERROR] Create failed: {e}")
            return False

    # R - Read
    def read(
        self,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: int = 0,
        sort: Optional[Sequence[Tuple[str, int]]] = None,
    ) -> List[Dict[str, Any]]:
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise ValueError("'query' must be a dict.")
        try:
            cursor = self.collection.find(query, projection)
            if sort:
                cursor = cursor.sort(list(sort))
            if isinstance(limit, int) and limit > 0:
                cursor = cursor.limit(limit)
            return [doc for doc in cursor]
        except PyMongoError as e:
            print(f"[ERROR] Read failed: {e}")
            return []

    # U - Update
    def update(
        self,
        query: Dict[str, Any],
        update_doc: Dict[str, Any],
        many: bool = False,
        upsert: bool = False,
    ) -> int:
        if not isinstance(query, dict) or not isinstance(update_doc, dict):
            raise ValueError("'query' and 'update_doc' must be dicts.")
        try:
            if many:
                res = self.collection.update_many(query, update_doc, upsert=upsert)
            else:
                res = self.collection.update_one(query, update_doc, upsert=upsert)
            return int(res.modified_count)
        except PyMongoError as e:
            print(f"[ERROR] Update failed: {e}")
            return 0

    # D - Delete
    def delete(self, query: Dict[str, Any], many: bool = False) -> int:
        if not isinstance(query, dict):
            raise ValueError("'query' must be a dict.")
        try:
            if many:
                res = self.collection.delete_many(query)
            else:
                res = self.collection.delete_one(query)
            return int(res.deleted_count)
        except PyMongoError as e:
            print(f"[ERROR] Delete failed: {e}")
            return 0

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"<AnimalShelter db={self.database.name!r} col={self.collection.name!r}>"
