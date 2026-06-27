from pymongo import MongoClient
from pymongo.collection import Collection
from typing import Optional, List, Dict
from datetime import datetime

class MongoDBClient:
    _instance = None
    _client = None
    _db = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str = 'localhost', port: int = 27017, db_name: str = 'douyin_live'):
        if self._client is None:
            self._client = MongoClient(host=host, port=port)
            self._db = self._client[db_name]
            self._create_all_indexes()

    @property
    def db(self):
        return self._db

    def get_danmu_collection(self) -> Collection:
        return self._db['danmu_log']

    def get_gift_collection(self) -> Collection:
        return self._db['gift_log']

    def get_hot_trend_collection(self) -> Collection:
        return self._db['hot_trend']

    def insert_danmu(self, data: dict):
        collection = self.get_danmu_collection()
        data['timestamp'] = data.get('timestamp', datetime.now())
        result = collection.insert_one(data)
        return result.inserted_id

    def bulk_insert_danmu(self, data_list: list):
        collection = self.get_danmu_collection()
        for data in data_list:
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now()
        result = collection.insert_many(data_list)
        return result.inserted_ids

    def insert_gift(self, data: dict):
        collection = self.get_gift_collection()
        data['timestamp'] = data.get('timestamp', datetime.now())
        required_fields = ['username', 'gift_name', 'count', 'room_id']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"缺少必填字段: {field}")
        result = collection.insert_one(data)
        return result.inserted_id

    def bulk_insert_gifts(self, data_list: list):
        collection = self.get_gift_collection()
        for data in data_list:
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now()
        result = collection.insert_many(data_list)
        return result.inserted_ids

    def insert_hot_snapshot(self, data: dict):
        collection = self.get_hot_trend_collection()
        data['timestamp'] = data.get('timestamp', datetime.now())
        required_fields = ['room_id', 'online_count', 'like_count']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"缺少必填字段: {field}")
        result = collection.insert_one(data)
        return result.inserted_id

    def bulk_insert_hot_snapshots(self, data_list: list):
        collection = self.get_hot_trend_collection()
        for data in data_list:
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now()
        result = collection.insert_many(data_list)
        return result.inserted_ids

    def get_hot_trend_by_room(self, room_id: str, start_time: datetime = None, end_time: datetime = None) -> List[Dict]:
        collection = self.get_hot_trend_collection()
        query = {'room_id': room_id}
        if start_time:
            query['timestamp'] = {'$gte': start_time}
        if end_time:
            query['timestamp'] = query.get('timestamp', {})
            query['timestamp']['$lte'] = end_time
        return list(collection.find(query).sort('timestamp', 1))

    def get_gifts_by_room(self, room_id: str, start_time: datetime = None, end_time: datetime = None) -> List[Dict]:
        collection = self.get_gift_collection()
        query = {'room_id': room_id}
        if start_time:
            query['timestamp'] = {'$gte': start_time}
        if end_time:
            query['timestamp'] = query.get('timestamp', {})
            query['timestamp']['$lte'] = end_time
        return list(collection.find(query).sort('timestamp', -1))

    def _create_all_indexes(self):
        danmu_collection = self.get_danmu_collection()
        danmu_collection.create_index('room_id')
        danmu_collection.create_index('timestamp')
        danmu_collection.create_index([('room_id', 1), ('timestamp', -1)])

        gift_collection = self.get_gift_collection()
        gift_collection.create_index('room_id')
        gift_collection.create_index('timestamp')
        gift_collection.create_index('username')
        gift_collection.create_index([('room_id', 1), ('timestamp', -1)])

        hot_trend_collection = self.get_hot_trend_collection()
        hot_trend_collection.create_index('room_id')
        hot_trend_collection.create_index('timestamp')
        hot_trend_collection.create_index([('room_id', 1), ('timestamp', -1)])

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self._instance = None


def get_mongo_client(host: str = 'localhost', port: int = 27017, db_name: str = 'douyin_live') -> MongoDBClient:
    return MongoDBClient(host=host, port=port, db_name=db_name)