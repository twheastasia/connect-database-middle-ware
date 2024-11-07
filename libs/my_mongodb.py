from typing import Optional, Any, List, Dict
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

class MyDatabaseMongodb:
    def __init__(self, 
                 host: str = 'localhost', 
                 port: int = 27017, 
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 database: str = 'default_db'):
        """Initialize MongoDB connection"""
        print(f"host: {host}, port: {port}, username: {username}, password: {password}, database: {database}")
        try:
            connection_string = f"mongodb://"
            if username and password:
                connection_string += f"{username}:{password}@"
            connection_string += f"{host}:{port}/{database}"

            self.client = MongoClient(connection_string)
            self.db: Database = self.client[database]
            print(f"Connected to MongoDB successfully!")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            raise

    def insert_one(self, collection_name: str, document: Dict) -> Optional[str]:
        """Insert a single document into collection"""
        try:
            collection: Collection = self.db[collection_name]
            result = collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            print(f"Error inserting document: {e}")
            return None

    def insert_many(self, collection_name: str, documents: List[Dict]) -> Optional[List[str]]:
        """Insert multiple documents into collection"""
        try:
            collection: Collection = self.db[collection_name]
            result = collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            print(f"Error inserting documents: {e}")
            return None

    def find_one(self, collection_name: str, query: Dict, projection: Dict = None) -> Optional[Dict]:
        """Find a single document"""
        print(f"collection_name: {collection_name}, query: {query}, projection: {projection}")
        try:
            collection: Collection = self.db[collection_name]
            return collection.find_one(query, projection)
        except Exception as e:
            print(f"Error finding document: {e}")
            return None

    def find_many(self, 
                 collection_name: str, 
                 query: Dict, 
                 projection: Dict = None,
                 sort: List = None,
                 limit: int = 0,
                 skip: int = 0) -> List[Dict]:
        """Find multiple documents"""
        try:
            collection: Collection = self.db[collection_name]
            cursor = collection.find(query, projection)
            
            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
        except Exception as e:
            print(f"Error finding documents: {e}")
            return []

    def update_one(self, 
                  collection_name: str, 
                  query: Dict, 
                  update: Dict, 
                  upsert: bool = False) -> bool:
        """Update a single document"""
        try:
            collection: Collection = self.db[collection_name]
            result = collection.update_one(query, update, upsert=upsert)
            return result.modified_count > 0 or (upsert and result.upserted_id is not None)
        except Exception as e:
            print(f"Error updating document: {e}")
            return False

    def update_many(self, 
                   collection_name: str, 
                   query: Dict, 
                   update: Dict, 
                   upsert: bool = False) -> int:
        """Update multiple documents"""
        try:
            collection: Collection = self.db[collection_name]
            result = collection.update_many(query, update, upsert=upsert)
            return result.modified_count
        except Exception as e:
            print(f"Error updating documents: {e}")
            return 0

    def delete_one(self, collection_name: str, query: Dict) -> bool:
        """Delete a single document"""
        try:
            collection: Collection = self.db[collection_name]
            result = collection.delete_one(query)
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting document: {e}")
            return False

    def delete_many(self, collection_name: str, query: Dict) -> int:
        """Delete multiple documents"""
        try:
            collection: Collection = self.db[collection_name]
            result = collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            print(f"Error deleting documents: {e}")
            return 0

    def count_documents(self, collection_name: str, query: Dict) -> int:
        """Count documents matching query"""
        try:
            collection: Collection = self.db[collection_name]
            return collection.count_documents(query)
        except Exception as e:
            print(f"Error counting documents: {e}")
            return 0

    def aggregate(self, collection_name: str, pipeline: List[Dict]) -> List[Dict]:
        """Perform aggregation pipeline"""
        try:
            collection: Collection = self.db[collection_name]
            return list(collection.aggregate(pipeline))
        except Exception as e:
            print(f"Error in aggregation: {e}")
            return []

    def create_index(self, collection_name: str, keys: List[tuple], unique: bool = False) -> bool:
        """Create an index on collection"""
        try:
            collection: Collection = self.db[collection_name]
            collection.create_index(keys, unique=unique)
            return True
        except Exception as e:
            print(f"Error creating index: {e}")
            return False

    def close(self) -> None:
        """Close MongoDB connection"""
        try:
            self.client.close()
        except Exception as e:
            print(f"Error closing MongoDB connection: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
