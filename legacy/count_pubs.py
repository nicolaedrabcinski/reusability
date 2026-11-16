import os

DATA_DIR = '../data'

# Путь к директории, в которой будем искать папки
directory_path = f'{DATA_DIR}/publications'

# Переменная для хранения общего количества XML файлов
total_xml_count = 0

# Обходим файловую систему
for root, dirs, files in os.walk(directory_path):
    for dir_name in dirs:
        # Полный путь к папке
        dir_path = os.path.join(root, dir_name)
        
        # Подсчет количества XML файлов в папке
        xml_count = len([file for file in os.listdir(dir_path) if file.endswith('.xml')])
        
        # Если в папке есть XML файлы, добавляем их количество к общему числу
        if xml_count > 0:
            total_xml_count += xml_count

# Выводим общее количество XML файлов
print("Общее количество XML файлов:", total_xml_count)