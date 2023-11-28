import hashlib
import os
import struct
import bson
import lz4

from lz4 import frame


class ShardKVStorage:
    # Инициализация объекта хранилища
    def __init__(self, filename_prefix, num_shards=256):
        self.filename_prefix = filename_prefix  # Префикс для имен файлов-шардов
        self.num_shards = num_shards  # Количество шардов (разделений)

    # Внутренний метод для получения имени файла-шарда на основе ключа
    def _get_shard_filename(self, key):
        hash_value = hashlib.md5(
            key.encode()).hexdigest()  # Получение хеша от ключа
        shard_num = int(hash_value,
                        16) % self.num_shards  # Определение номера шарда на основе хеша
        return f"{self.filename_prefix}_shard_{shard_num}.kvs"  # Формирование имени файла

    # Метод для установки значения по ключу
    def set(self, key, value):
        filename = self._get_shard_filename(
            key)  # Получение имени файла-шарда
        with open(filename,
                  'ab') as f:  # Открытие файла для добавления данных
            entry = bson.dumps(
                {key: value})  # Сериализация данных в формат BSON
            compressed = lz4.frame.compress(entry)  # Сжатие данных

            # Сохранение размера сжатых данных и самих данных
            f.write(struct.pack("I",
                                len(compressed)))  # Запись размера сжатых данных
            f.write(compressed)  # Запись сжатых данных

    # Метод для установки нескольких значений
    def set_multiple(self, key_value_dict):
        for key, value in key_value_dict.items():  # Проход по словарю и установка каждой пары ключ-значение
            self.set(key, value)

    # Метод для получения значения по ключу
    def get(self, key, default=None):
        filename = self._get_shard_filename(
            key)  # Получение имени файла-шарда
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                while True:
                    size_data = f.read(4)  # Чтение размера сжатых данных
                    if not size_data:
                        return default
                    size = struct.unpack("I", size_data)[0]
                    chunk = f.read(size)  # Чтение сжатых данных
                    data = bson.loads(lz4.frame.decompress(
                        chunk))  # Расшифровка и десериализация данных
                    if key in data:
                        return data[key]
        return default

    # Метод для удаления значения по ключу.
    def delete(self, key):
        filename = self._get_shard_filename(
            key)  # Получение имени файла-шарда
        if os.path.exists(filename):
            new_data = {}
            with open(filename, 'rb') as f:
                while True:
                    size_data = f.read(4)
                    if not size_data:
                        break
                    size = struct.unpack("I", size_data)[0]
                    chunk = f.read(size)
                    decompressed = lz4.frame.decompress(
                        chunk)  # Расшифровка данных
                    data = bson.loads(decompressed)
                    new_data.update(
                        data)  # Добавление данных в временный словарь

            data.pop(key, None)  # Удаление ключа

            with open(filename,
                      'wb') as f:  # Запись обновленных данных обратно в файл
                for key, value in new_data.items():
                    entry = bson.dumps({key: value})
                    compressed = lz4.frame.compress(entry)
                    f.write(struct.pack("I", len(compressed)))
                    f.write(compressed)

    # Метод для получения всех ключей в хранилище
    def get_all_keys(self):
        all_keys = []
        for shard_num in range(
                self.num_shards):  # Проход по всем файлам-шардам
            filename = f"{self.filename_prefix}_shard_{shard_num}.kvs"
            if os.path.exists(filename):
                with open(filename, 'rb') as f:
                    while True:
                        size_data = f.read(4)
                        if not size_data:
                            break
                        size = struct.unpack("I", size_data)[0]
                        chunk = f.read(size)
                        decompressed = lz4.frame.decompress(chunk)
                        data = bson.loads(decompressed)
                        for key in data.keys():
                            if data[key] is not None:
                                all_keys.append(key)  # Добавление ключей из файла в общий список
        return list(set(all_keys))  # Удаление дубликатов ключей

    def search_keys_for_prefix(self, prefix):
        result = []
        for shard_num in range(
                self.num_shards):  # Проход по всем файлам-шардам
            filename = f"{self.filename_prefix}_shard_{shard_num}.kvs"
            if os.path.exists(filename):
                with open(filename, 'rb') as f:
                    while True:
                        size_data = f.read(4)
                        if not size_data:
                            break
                        size = struct.unpack("I", size_data)[0]
                        chunk = f.read(size)
                        decompressed = lz4.frame.decompress(chunk)
                        data = bson.loads(decompressed)
                    for key in list(data.keys()):
                        if key.startswith(prefix):
                            result.append(key)
        return result

    # Метод для поиска ключей по значению
    def search_keys_for_value(self, target_value):
        results = []
        for shard_num in range(
                self.num_shards):  # Проход по всем файлам-шардам
            filename = f"{self.filename_prefix}_shard_{shard_num}.kvs"
            if os.path.exists(filename):
                with open(filename, 'rb') as f:
                    while True:
                        size_data = f.read(4)
                        if not size_data:
                            break
                        size = struct.unpack("I", size_data)[0]
                        chunk = f.read(size)
                        decompressed = lz4.frame.decompress(chunk)
                        data = bson.loads(decompressed)
                    for key, value in data:
                        if value == target_value:
                            results.append(key)
        return results

    def save(self):
        pass
