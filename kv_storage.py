import time
import lz4
from lz4 import frame
import hashlib
import struct
import os
import bson
import lz4.frame


class OriginalKVStorage:
    # Инициализация объекта хранилища
    def __init__(self, filename):
        self.filename = filename  # Имя файла, где будет сохраняться база данных
        self.data = {}  # Словарь для хранения данных из файла
        self.buffer = {}  # Буфер для временного хранения данных

        # Если файл существует, загрузить данные из него
        if os.path.exists(filename):
            self.load()

    # Метод для добавления значения по ключу в буфер
    def set(self, key, value):
        self.buffer[key] = value

    # Метод для добавления нескольких значений
    def set_multiple(self, key_value_dict):
        for key, value in key_value_dict.items():
            self.set(key, value)
        self.commit()  # Применить изменения из буфера

    # Метод для получения значения по ключу
    def get(self, key, default=None):
        if key in self.buffer:  # Если ключ в буфере, вернуть значение из него
            return self.buffer[key]
        return self.data.get(key,
                             default)  # Иначе взять значение из основного хранилища

    # Метод для удаления значения по ключу
    def delete(self, key):
        self.buffer[
            key] = None  # Установка значения None в буфере указывает на необходимость удаления

    # Метод для получения всех ключей в хранилище
    def get_all_keys(self):
        # Объединение ключей из основного хранилища и буфера (если значения не None)
        all_keys = list(self.data.keys()) + [k for k, v in self.buffer.items()
                                             if v is not None]
        return list(set(all_keys))  # Удаление дубликатов ключей

    # Метод для применения изменений из буфера в основное хранилище
    def commit(self):
        for key, value in self.buffer.items():
            if value is None:  # Если значение None, удалить ключ
                self.data.pop(key, None)
            else:
                self.data[key] = value
        self.buffer.clear()  # Очистить буфер
        self.save()  # Сохранить изменения в файл

    # Метод для сохранения данных хранилища в файл
    def save(self):
        with open(self.filename, 'wb') as f:
            packed_data = bson.dumps(
                self.data)  # Сериализация данных в формат BSON
            compressed = lz4.frame.compress(packed_data)  # Сжатие данных
            f.write(compressed)  # Запись в файл

    # Метод для загрузки данных из файла
    def load(self):
        with open(self.filename, 'rb') as f:
            compressed = f.read()  # Чтение сжатых данных
            decompressed = lz4.frame.decompress(
                compressed)  # Расшифровка данных
            self.data = bson.loads(
                decompressed)  # Десериализация данных из формата BSON

    # Метод для префиксного поиска по ключам
    def search_key_for_prefix(self, prefix):
        results = {}
        for key, value in self.data and self.buffer:
            if key.startswith(prefix):
                results[key] = value
        return results

    # Метод для поиска всех ключей по заданному значению
    def search_keys_for_value(self, target_value):
        results = [key for key, value in self.data and self.buffer if value == target_value]
        return results



class ShardKVStorage:
    # Инициализация объекта хранилища
    def __init__(self, filename_prefix, num_shards=256):
        self.filename_prefix = filename_prefix  # Префикс для имен файлов-шардов
        self.num_shards = num_shards  # Количество шардов (разделений)
        self.buffer = {}  # Буфер для временного хранения данных (не используется в представленном коде)

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

            new_data.pop(key, None)  # Удаление ключа

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
                        all_keys.extend(
                            data.keys())  # Добавление ключей из файла в общий список
        return list(set(all_keys))  # Удаление дубликатов ключей

    def commit(self):
        pass

    def search_keys_for_prefix(self, prefix):
        result = {}
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
                        if key.startswith(prefix):
                            result[key] = value
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