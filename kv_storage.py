import os
import zlib
import bson
from datetime import datetime
import lz4.frame
import hashlib

class Hasher:
    @staticmethod
    def hash(key):
        if isinstance(key, str):
            return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
        else:
            raise TypeError("Key must be a string")

class KVStorage:
    def __init__(self, filename):
        self.filename = filename
        self.data = {}
        self.hasher = Hasher()
        if os.path.exists(filename):
            self.load()

    def set(self, key, value):
        hashed_key = self.hasher.hash(key)
        self.data[hashed_key] = value
        self.save()

    def set_multiple(self, key_value_dict):
        for key, value in key_value_dict.items():
            self.set(key, value)

    def get(self, key, default=None):
        hashed_key = self.hasher.hash(key)
        return self.data.get(hashed_key, default)

    def delete(self, key):
        hashed_key = self.hasher.hash(key)
        if hashed_key in self.data:
            del self.data[hashed_key]
            self.save()

    def get_all_keys(self):
        return [key for key in self.data.keys()]

    def save(self):
        with open(self.filename, 'wb') as f:
            packed_data = bson.dumps(self.data)
            compressed = lz4.frame.compress(packed_data)
            f.write(compressed)

    def load(self):
        with open(self.filename, 'rb') as f:
            compressed = f.read()
            decompressed = lz4.frame.decompress(compressed)
            self.data = bson.loads(decompressed)


# Пример использования:
storage = KVStorage('data.kvs')
f = open('log2.txt')
value3 = f.read()
time1 = datetime.now()
storage.set('key1', 'value1')
storage.set('key2', 'value2')
storage.set('key3', value3)
print(storage.get_all_keys())
print(datetime.now() - time1)
a = storage.get('key1')
print(datetime.now() - time1)
f.close()


