import os
import unittest
import shutil

from storage_arch import kv_storage


def delete_test_files():
    os.chdir('..')
    path = os.getcwd()
    shutil.rmtree(path + "\\\\test_storage")


class TestKVStorage(unittest.TestCase):
    def setUp(self):
        os.makedirs("test_storage")
        os.chdir("test_storage")
        self.storage = kv_storage.KVStorageHandler("test_original", "test_shard", 10)

    def test_mass_insert(self):
        data = {'key1': 'value1',
                'key2': 'value2',
                'key3': 'value3',
                'key4': 'value4',
                'key5': 'value5',
                'key6': 'value6',
                'key7': 'value7',
                'key8': 'value8',
                'key9': 'value9'}
        self.storage.set_multiple(data)
        test_list = []
        for k, v in data.items():
            test_list.append(self.storage.get(k))

        self.assertEquals(test_list.sort(), list(data.values()).sort())
        delete_test_files()

    def test_find_keys_for_prefix(self):
        data = {'key1': 'value1',
                'key2': 'value2',
                'key3': 'value3',
                'key4': 'value4',
                'kkeeyy5': 'value5',
                'kkeeyy6': 'value6',
                'kkeeyy7': 'value7',
                'kkeeyy8': 'value8',
                'kkeeyy9': 'value9'}
        self.storage.set_multiple(data)
        key_prefix_keys = self.storage.search_key_for_prefix('key')
        kkeeyy_prefix_keys = self.storage.search_key_for_prefix('kkeeyy')

        self.assertEquals(['key1',
                           'key2',
                           'key3',
                           'key4'].sort(),
                          list(key_prefix_keys).sort())

        self.assertEquals(['kkeeyy5',
                           'kkeeyy6',
                           'kkeeyy7',
                           'kkeeyy8',
                           'kkeeyy9'].sort(),
                          list(kkeeyy_prefix_keys).sort())

        delete_test_files()

    def test_find_keys_for_value(self):
        data = {'key1': 'value1',
                'key2': 'value1',
                'key3': 'value2',
                'key4': 'value2',
                'key5': 'value2',
                'key6': 'value3',
                'key7': 'value3',
                'key8': 'value3',
                'key9': 'value3'}
        self.storage.set_multiple(data)
        keys_for_value1 = self.storage.search_keys_for_value("value1")
        keys_for_value2 = self.storage.search_keys_for_value("value2")
        keys_for_value3 = self.storage.search_keys_for_value("value3")

        delete_test_files()
        self.assertEquals(["key1", "key2"].sort(), keys_for_value1.sort())
        self.assertEquals(["key3", "key4", "key5"].sort(), keys_for_value2.sort())
        self.assertEquals(["key6", "key7", "key8", "key9"].sort(), keys_for_value3.sort())

    def test_get_all_keys(self):
        data = {'key1': 'value1',
                'key2': 'value2',
                'key3': 'value3',
                'key4': 'value4',
                'key5': 'value5',
                'key6': 'value6',
                'key7': 'value7',
                'key8': 'value8',
                'key9': 'value9'}
        self.storage.set_multiple(data)
        values = self.storage.get_all_keys()

        delete_test_files()
        self.assertEquals(values.sort(), list(data.keys()).sort())

    def test_delete(self):
        data = {'key1': 'value1',
                'key2': 'value2',
                'key3': 'value3',
                'key4': 'value4',
                'key5': 'value5',
                'key6': 'value6',
                'key7': 'value7',
                'key8': 'value8',
                'key9': 'value9'}
        self.storage.set_multiple(data)
        self.storage.delete("key1")
        self.storage.delete("key5")
        key1_value = self.storage.get("key1")
        key5_value = self.storage.get("key5")

        delete_test_files()
        self.assertEquals(None, key1_value)
        self.assertEquals(None, key5_value)

    def test_work_with_large_file(self):
        os.chdir('..')
        filepath = os.path.join(os.getcwd(), "log2.txt")
        os.chdir("test_storage")
        file_size = os.path.getsize(filepath)
        with open(filepath, 'r') as f:
            content = f.read()

        self.storage.set('key_largefile', content)
        archived_file_size = self.storage._shard_storage_total_size()
        kv_content = self.storage.get('key_largefile')
        delete_test_files()
        self.assertLess(archived_file_size, file_size)
        self.assertEquals(content, kv_content)


if __name__ == "__main__":
    unittest.main()
