import redis
from typing import Optional, Any, List, Dict
import json

class MyDatabaseRedis:
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, password: Optional[str] = None):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True  # Automatically decode responses to strings
        )

    def query(self, sql):
        result = self.redis_client.execute_command(sql)
        return result

    def set_value(self, key: str, value: Any, expiry_seconds: Optional[int] = None) -> bool:
        """Set a key-value pair, optionally with expiry time"""
        try:
            # Convert non-string values to JSON
            if not isinstance(value, (str, int, float)):
                value = json.dumps(value)
            
            self.redis_client.set(key, value, ex=expiry_seconds)
            return True
        except Exception as e:
            print(f"Error setting value: {e}")
            return False

    def get_value(self, key: str) -> Any:
        """Get value for a key"""
        try:
            value = self.redis_client.get(key)
            if value is None:
                return None
            
            # Try to parse as JSON, if fails return as is
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            print(f"Error getting value: {e}")
            return None

    def delete_key(self, key: str) -> bool:
        """Delete a key"""
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            print(f"Error deleting key: {e}")
            return False

    def set_hash(self, hash_name: str, mapping: Dict) -> bool:
        """Set multiple hash fields"""
        try:
            self.redis_client.hset(hash_name, mapping=mapping)
            return True
        except Exception as e:
            print(f"Error setting hash: {e}")
            return False

    def get_hash(self, hash_name: str) -> Dict:
        """Get all fields in a hash"""
        try:
            return self.redis_client.hgetall(hash_name)
        except Exception as e:
            print(f"Error getting hash: {e}")
            return {}

    def push_to_list(self, list_name: str, *values: Any) -> bool:
        """Push values to a list"""
        try:
            self.redis_client.rpush(list_name, *values)
            return True
        except Exception as e:
            print(f"Error pushing to list: {e}")
            return False

    def get_list(self, list_name: str) -> List:
        """Get all values in a list"""
        try:
            return self.redis_client.lrange(list_name, 0, -1)
        except Exception as e:
            print(f"Error getting list: {e}")
            return []

    def add_to_set(self, set_name: str, *values: Any) -> bool:
        """Add values to a set"""
        try:
            self.redis_client.sadd(set_name, *values)
            return True
        except Exception as e:
            print(f"Error adding to set: {e}")
            return False

    def get_set_members(self, set_name: str) -> set:
        """Get all members of a set"""
        try:
            return self.redis_client.smembers(set_name)
        except Exception as e:
            print(f"Error getting set members: {e}")
            return set()

    def key_exists(self, key: str) -> bool:
        """Check if a key exists"""
        return bool(self.redis_client.exists(key))

    def set_expiry(self, key: str, seconds: int) -> bool:
        """Set expiry time for a key"""
        try:
            return bool(self.redis_client.expire(key, seconds))
        except Exception as e:
            print(f"Error setting expiry: {e}")
            return False

    def get_ttl(self, key: str) -> int:
        """Get remaining time to live for a key in seconds"""
        try:
            return self.redis_client.ttl(key)
        except Exception as e:
            print(f"Error getting TTL: {e}")
            return -1

    def close(self) -> None:
        """Close the Redis connection"""
        try:
            self.redis_client.close()
        except Exception as e:
            print(f"Error closing Redis connection: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        print("Redis connection closed")
