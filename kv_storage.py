import json
import os
import zlib


class KVStorage:
    def __init__(self, filename):
        self.filename = filename
        self.data = {}
        if os.path.exists(filename):
            self.load()

    def set(self, key, value):
        self.data[key] = value
        self.save()

    def get(self, key):
        return self.data.get(key, None)

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            self.save()

    def save(self):
        with open(self.filename, 'wb') as f:
            compressed = zlib.compress(json.dumps(self.data).encode('utf-8'))
            f.write(compressed)

    def load(self):
        with open(self.filename, 'rb') as f:
            compressed = f.read()
            decompressed = zlib.decompress(compressed).decode('utf-8')
            self.data = json.loads(decompressed)


kv = KVStorage('storage.json')
kv.set('key1', 'value1')
kv.set('key2', 'value2')
kv.set('key3', 'value3')

print(kv.get('key1'))
print(kv.get('key2'))
print(kv.get('key3'))
print(kv.get('key5'))
