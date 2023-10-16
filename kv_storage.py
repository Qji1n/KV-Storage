import os
import bson
from datetime import datetime
import lz4.frame
import hashlib
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, Toplevel
import base64
import math
import threading


class Hasher:
    @staticmethod
    def hash(key):
        if isinstance(key, str):
            return str(int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16))
        else:
            raise TypeError("Key must be a string")




class FileWrapper:
    @staticmethod
    def encode_file_to_string(filepath):
        with open(filepath, 'rb') as file:
            return base64.b64encode(file.read()).decode('utf-8')

    @staticmethod
    def decode_string_to_file(data, filepath):
        with open(filepath, 'wb') as file:
            file.write(base64.b64decode(data))


class KVStorageGUI(tk.Tk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("KV Storage")
        self.geometry("600x600")

        self.storage = None

        self.filepath_label = tk.Label(self, text="No storage selected")
        self.filepath_label.pack(pady=20)

        self.select_button = tk.Button(self, text="Select Storage",
                                       command=self.select_storage)
        self.select_button.pack()

        self.create_button = tk.Button(self, text="Create New Storage",
                                       command=self.create_new_storage)
        self.create_button.pack(pady=10)

        self.set_button = tk.Button(self, text="Set Key-Value",
                                    command=self.set_key_value,
                                    state=tk.DISABLED)
        self.set_button.pack(pady=5)

        self.set_multiple_button = tk.Button(self,
                                             text="Set Multiple Key-Values",
                                             command=self.set_multiple_key_values,
                                             state=tk.DISABLED)
        self.set_multiple_button.pack(pady=5)

        self.get_button = tk.Button(self, text="Get Value by Key",
                                    command=self.get_value_by_key,
                                    state=tk.DISABLED)
        self.get_button.pack(pady=5)

        self.get_all_keys_button = tk.Button(self, text="Get All Keys",
                                             command=self.get_all_keys,
                                             state=tk.DISABLED)
        self.get_all_keys_button.pack(pady=5)

        self.keys_listbox = tk.Listbox(self, height=10)
        self.keys_listbox.pack(pady=10)

        self.pagination_frame = tk.Frame(self)
        self.pagination_frame.pack(pady=20)

        self.prev_button = tk.Button(self.pagination_frame, text="Prev",
                                     command=self.prev_page, state=tk.DISABLED)
        self.prev_button.pack(side="left", padx=5)

        self.next_button = tk.Button(self.pagination_frame, text="Next",
                                     command=self.next_page, state=tk.DISABLED)
        self.next_button.pack(side="left", padx=5)

        self.current_page = 0
        self.keys_per_page = 10

    def update_buttons_state(self):
        if self.current_page == 0:
            self.prev_button.config(state=tk.DISABLED)
        else:
            self.prev_button.config(state=tk.NORMAL)

        if len(self.storage.get_all_keys()) <= (
                self.current_page + 1) * self.keys_per_page:
            self.next_button.config(state=tk.DISABLED)
        else:
            self.next_button.config(state=tk.NORMAL)


    def get_all_keys(self):
        total_keys = self.storage.get_all_keys()
        total_pages = math.ceil(len(total_keys) / self.keys_per_page)

        start_index = self.current_page * self.keys_per_page
        end_index = start_index + self.keys_per_page
        displayed_keys = total_keys[start_index:end_index]

        self.keys_listbox.delete(0, tk.END)
        for key in displayed_keys:
            self.keys_listbox.insert(tk.END, key)

        self.prev_button.config(
            state=tk.NORMAL if self.current_page > 0 else tk.DISABLED)
        self.next_button.config(
            state=tk.NORMAL if self.current_page < total_pages - 1 else tk.DISABLED)

    def prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.get_all_keys()

    def next_page(self):
        total_keys = self.storage.get_all_keys()
        total_pages = math.ceil(len(total_keys) / self.keys_per_page)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.get_all_keys()

    def select_storage(self):
        filepath = filedialog.askopenfilename()
        if not filepath:
            return
        self.storage = KVStorage(filepath)
        self.filepath_label.configure(text=f"Selected storage: {filepath}")
        self.set_button.configure(state=tk.NORMAL)
        self.get_button.configure(state=tk.NORMAL)
        self.get_all_keys_button.configure(state=tk.NORMAL)
        self.set_multiple_button.configure(state=tk.NORMAL)
        self.update_buttons_state()  # Обновляем состояние кнопок "Prev" и "Next"

    def create_new_storage(self):
        filepath = filedialog.asksaveasfilename(defaultextension=".kvs",
                                                filetypes=[
                                                    ("KV Storage", "*.kvs")])
        if not filepath:
            return
        self.storage = KVStorage(filepath)
        self.filepath_label.configure(text=f"Created new storage: {filepath}")
        self.set_button.configure(state=tk.NORMAL)
        self.get_button.configure(state=tk.NORMAL)
        self.get_all_keys_button.configure(state=tk.NORMAL)
        self.set_multiple_button.configure(state=tk.NORMAL)
        self.update_buttons_state()  # Обновляем состояние кнопок "Prev" и "Next"

    def set_key_value(self):
        key = simpledialog.askstring("Input", "Enter the key:")
        if not key:
            return
        value_type = messagebox.askquestion("Choose value type", "Do you want to set the value as a file?", icon='warning')
        if value_type == 'yes':
            value_path = filedialog.askopenfilename()
            if not value_path:
                return
            value = FileWrapper.encode_file_to_string(value_path)
        else:
            value = simpledialog.askstring("Input", "Enter the value:")
        self.storage.set(key, value)
        messagebox.showinfo("Success", f"Set value for key '{key}'")

    def set_multiple_key_values(self):
        window = Toplevel(self)
        window.title("Set Multiple Key-Values")
        window.geometry("400x300")

        # Add widgets to the window for setting multiple key-values

    def get_value_by_key(self):
        key = simpledialog.askstring("Input", "Enter the key to fetch:")
        if not key:
            return
        value = self.storage.get(key)
        if value is not None:
            value_type = messagebox.askquestion("Choose value type", "Do you want to save the value as a file?", icon='warning')
            if value_type == 'yes':
                save_path = filedialog.asksaveasfilename()
                if not save_path:
                    return
                FileWrapper.decode_string_to_file(value, save_path)
                messagebox.showinfo("Success", f"Value saved to {save_path}")
            else:
                messagebox.showinfo("Value", f"Value for '{key}':\n\n{value}")
        else:
            messagebox.showerror("Error", f"No value found for key '{key}'")

'''
if __name__ == "__main__":
    app = KVStorageGUI()
    app.mainloop()
'''

class KVStorage:
    def __init__(self, filename):
        self.filename = filename
        self.data = {}
        self.hasher = Hasher()
        if os.path.exists(filename):
            self.load()

    def set(self, key, value, multiply = False):
        hashed_key = self.hasher.hash(key)
        self.data[hashed_key] = {'original_key': key, 'value': value}
        if (not multiply):
            self.save()

    def set_multiple(self, key_value_dict):
        for key, value in key_value_dict.items():
            self.set(key, value, multiply=True)
        self.save()

    def get(self, key, default=None):
        hashed_key = self.hasher.hash(key)
        return self.data.get(hashed_key, {}).get('value', default)

    def delete(self, key):
        hashed_key = self.hasher.hash(key)
        if hashed_key in self.data:
            del self.data[hashed_key]
            self.save()

    def get_all_keys(self):
        return [item['original_key'] for item in self.data.values()]

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

time1 = datetime.now()
storage = KVStorage('data_small.kvs')
f = open('log2.txt')
value1001 = f.read()
print(datetime.now() - time1)
value_dict = {}
for i in range(1, 1000000):
    value_dict['key {i}'.format(i = i)] = 'value {i}'.format(i = i)
print(datetime.now() - time1)
storage.set('key 1001', value1001)
print(datetime.now() - time1)
storage.set_multiple(value_dict)
print(datetime.now() - time1)
a = storage.get('key 783')
print(datetime.now() - time1)
f.close()
