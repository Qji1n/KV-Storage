import time
import os

from storage_arch import kv_storage


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
original_storage = kv_storage.OriginalKVStorage('original_data.kvs')
shard_storage = kv_storage.ShardKVStorage('shard_data.kvs')

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


# Тест получения всех ключей
original_get_all_keys_time = test_get_all_keys(original_storage)
shard_get_all_keys_time = test_get_all_keys(shard_storage)
print(f"Original Get all keys Time for Large File: {original_get_all_keys_time} seconds")
print(f"Shard Get all keys Time for Large File: {shard_get_all_keys_time} seconds")