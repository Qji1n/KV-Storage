import time
import os
import lz4
from lz4 import frame
import bson
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


def test_mass_insert(storage, data):
    start_time = time.time()
    if hasattr(storage, 'set_multiple'):
        storage.set_multiple(data)
    else:
        for key, value in data.items():
            storage.put(key, value)
    end_time = time.time()
    return end_time - start_time

def test_mass_get(storage, keys):
    start_time = time.time()
    for key in keys:
        storage.get(key)
    end_time = time.time()
    return end_time - start_time

def generate_data(num_entries=1000000):
    return {f'key {i}': f'value {i}' for i in range(num_entries)}

def test_large_file_insert(storage, filepath, repetitions=5):
    with open(filepath, 'r') as f:
        content = f.read()

    start_time = time.time()
    for i in range(repetitions):
        storage.set(f'key_largefile_{i}', content)
    storage.commit()

    end_time = time.time()

    return end_time - start_time

def test_large_file_get(storage, repetitions=5):
    start_time = time.time()
    for i in range(repetitions):
        _values = storage.get(f'key_largefile_{i}')
    end_time = time.time()

    return end_time - start_time


def test_get_all_keys(storage, repetitions = 5):
    start_time = time.time()
    _values = storage.get_all_keys()
    end_time = time.time()

    return end_time - start_time


filepath = os.path.join(os.getcwd(), "log2.txt")


# Инициализируем хранилища
original_storage = OriginalKVStorage('original_data.kvs')
shard_storage = ShardKVStorage('shard_data.kvs')

# Тест массовой вставки большого файла
original_insert_time = test_large_file_insert(original_storage, filepath)
shard_insert_time = test_large_file_insert(shard_storage, filepath)

print(f"Original Insert Time for Large File: {original_insert_time} seconds")
print(f"Shard Insert Time for Large File: {shard_insert_time} seconds")

# Тест массового запроса большого файла
original_get_time = test_large_file_get(original_storage)
shard_get_time = test_large_file_get(shard_storage)

print(f"Original Get Time for Large File: {original_get_time} seconds")
print(f"Shard Get Time for Large File: {shard_get_time} seconds")


#Тест получения всех ключей
original_get_all_keys_time = test_get_all_keys(original_storage)
shard_get_all_keys_time = test_get_all_keys(shard_storage)
print(f"Original Get all keys Time for Large File: {original_get_all_keys_time} seconds")
print(f"Shard Get all keys Time for Large File: {shard_get_all_keys_time} seconds")