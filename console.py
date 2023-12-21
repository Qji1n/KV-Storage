from storage_arch.kv_storage import KVStorageHandler


def print_menu():
    print("\nВыберите действие:")
    print("1. Добавить значение")
    print("2. Получить значение")
    print("3. Удалить значение")
    print("4. Получить все ключи")
    print("5. Найти ключи по префиксу")
    print("6. Найти ключи по значению")
    print("7. Выход")


def main():
    storage = KVStorageHandler("my_database.kvs", "shard")  # Используйте ShardKVStorage для шардированного хранилища

    while True:
        print_menu()
        choice = input("Введите номер действия: ")

        if choice == '1':
            single_or_multiple = input("Добавить одно значение (1) или несколько значений (2)? ")
            if single_or_multiple == '1':
                key = input("Введите ключ: ")
                value = input("Введите значение: ")
                storage.set(key, value)
                print("\nЗначение добавлено.")
            elif single_or_multiple == '2':
                n = int(input("Сколько значений добавить? "))
                dict_keys_values = {}
                for i in range(n):
                    key = input(f"Введите ключ {i+1}: ")
                    value = input(f"Введите значение {i+1}: ")
                    dict_keys_values[key] = value
                storage.set_multiple(dict_keys_values)
                print("\nЗначения добавлены.")
            else:
                print("\nНеверный выбор.")

        elif choice == '2':
            key = input("Введите ключ: ")
            value = storage.get(key)
            if value is not None:
                print(f"\nЗначение: {value}")
            else:
                print("\nЗначение не найдено.")

        elif choice == '3':
            key = input("Введите ключ для удаления: ")
            if key in storage.get_all_keys():
                storage.delete(key)
                print("\nКлюч удалён.")
            else:
                print("\nКлюч не найден.")

        elif choice == '4':
            keys = storage.get_all_keys()
            if keys:
                print("\nВсе ключи:", keys)
            else:
                print("\nХранилище пусто.")

        elif choice == '5':
            prefix = input("Введите префикс: ")
            keys = storage.search_key_for_prefix(prefix)
            if keys:
                print("\nНайденные ключи:", keys)
            else:
                print("\nКлючи с таким префиксом не найдены.")

        elif choice == '6':
            value = input("Введите значение для поиска: ")
            keys = storage.search_keys_for_value(value)
            if keys:
                print("\nНайденные ключи:", keys)
            else:
                print("\nКлючи с таким значением не найдены.")

        elif choice == '7':
            print("\nВыход из хранилища.")
            storage.save()
            break

        else:
            print("\nНеверный выбор, попробуйте ещё раз.")


if __name__ == "__main__":
    main()
