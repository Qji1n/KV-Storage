import os
import sys

from storage_arch.original_kvs import OriginalKVStorage
from storage_arch.shard_kvs import ShardKVStorage


class MemoryLimitExceededError(Exception):
    """Исключение, вызываемое при превышении лимита памяти."""


class KVStorageHandler:
    def __init__(self, original_filename, shard_prefix,
                 ram_limit=500 * 1024 * 1024,
                 disk_limit=2 * 1024 * 1024 * 1024, num_shards=256):
        self.original_storage = OriginalKVStorage(original_filename)
        self.shard_storage = ShardKVStorage(shard_prefix, num_shards)

        self.ram_limit = ram_limit
        self.disk_limit = disk_limit


    def _current_ram_usage(self):
        return sum(sys.getsizeof(key) + sys.getsizeof(value) for key, value in
                   self.original_storage.data.items())

    def _choose_storage(self, key, value):
        size = sys.getsizeof(key) + sys.getsizeof(value)
        if (size > self.ram_limit - self._current_ram_usage()) \
                or (size >  50 * 1024 * 1024):
            if self._shard_storage_total_size() + size > self.disk_limit:
                raise MemoryLimitExceededError("Memory limits exceeded")
            return self.shard_storage
        else:
            return self.original_storage

    def _shard_storage_total_size(self):
        total_size = 0
        for i in range(self.shard_storage.num_shards):
            shard_filename = f"{self.shard_storage.filename_prefix}_shard_{i}.kvs"
            if os.path.exists(shard_filename):
                total_size += os.path.getsize(shard_filename)
        return total_size

    def set(self, key, value):
        if self.get(key) != None:
            self.delete(key)
        storage = self._choose_storage(key, value)
        storage.set(key, value)

    def get(self, key, default=None):
        if key in self.original_storage.data:
            return self.original_storage.get(key, default)
        return self.shard_storage.get(key, default)

    def delete(self, key):
        if key in self.original_storage.data:
            self.original_storage.delete(key)
        else:
            self.shard_storage.delete(key)

    def get_all_keys(self):
        return \
            list(self.original_storage.get_all_keys()
                 + self.shard_storage.get_all_keys())

    def set_multiple(self, key_value_dict):
        for key, value in key_value_dict.items():
            self.set(key, value)

    def search_key_for_prefix(self, prefix):
        original_results = self.original_storage.search_keys_for_prefix(prefix)
        shard_results = self.shard_storage.search_keys_for_prefix(prefix)
        return original_results + shard_results

    def search_keys_for_value(self, target_value):
        original_keys = self.original_storage.search_keys_for_value(
            target_value)
        shard_keys = self.shard_storage.search_keys_for_value(target_value)
        return original_keys + shard_keys

    # Дополнительные функции для сохранения и загрузки
    def save(self):
        self.original_storage.save()

    def load(self):
        self.original_storage.load()
