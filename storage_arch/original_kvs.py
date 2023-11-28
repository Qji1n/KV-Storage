import os
import bson
import lz4

from lz4 import frame


class OriginalKVStorage:
    # Инициализация объекта хранилища
    def __init__(self, filename):
        self.filename = filename  # Имя файла, где будет сохраняться база данных
        self.data = {}  # Словарь для хранения данных из файла

        # Если файл существует, загрузить данные из него
        if os.path.exists(filename):
            self.load()

    # Метод для добавления значения по ключу в буфер
    def set(self, key, value):
        self.data[key] = value

    # Метод для добавления нескольких значений
    def set_multiple(self, key_value_dict):
        for key, value in key_value_dict.items():
            self.set(key, value)

    # Метод для получения значения по ключу
    def get(self, key, default=None):
        if key in self.data.keys():  # Если ключ в ram, вернуть значение из него
            return self.data[key]
        return default

    # Метод для удаления значения по ключу
    def delete(self, key):
        if key in self.data.keys():
            self.data[key] = None
            # Установка значения None в буфере указывает на необходимость удаления

    # Метод для получения всех ключей в хранилище
    def get_all_keys(self):
        all_keys = []
        for key in list(self.data.keys()):
            if self.data[key] is not None:
                all_keys.append(key)
        return all_keys

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
    def search_keys_for_prefix(self, prefix):
        result = []
        for key in list(self.data.keys()):
            if key.startswith(prefix):
                result.append(key)
        return result

    # Метод для поиска всех ключей по заданному значению
    def search_keys_for_value(self, target_value):
        results = [key for key, value in self.data.items() if value == target_value]
        return results
