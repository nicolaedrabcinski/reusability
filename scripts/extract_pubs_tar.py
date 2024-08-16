import os
import tarfile

def extract_tar_gz(source_folder, destination_folder):
    # Проверяем, существует ли целевая папка, и создаем её, если нет
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Рекурсивно перебираем все файлы и папки в исходной папке
    for root, dirs, files in os.walk(source_folder):
        for file_name in files:
            # Проверяем, является ли файл .tar.gz
            if file_name.endswith('.tar.gz'):
                # Полный путь к исходному файлу
                file_path = os.path.join(root, file_name)
                # Имя архива без расширения .tar.gz
                archive_name = file_name[:-7]
                # Определяем целевую директорию для разархивирования
                target_folder = os.path.join(destination_folder, archive_name)
                # Проверяем, существует ли целевая директория, и создаем её, если нет
                if not os.path.exists(target_folder):
                    os.makedirs(target_folder)
                # Открываем .tar.gz файл
                with tarfile.open(file_path, 'r:gz') as tar:
                    # Извлекаем все файлы в целевую директорию
                    tar.extractall(path=target_folder)
                print(f"Разархивирован: {file_path} в {target_folder}")


source_folder = '/home/nicolaedrabcinski/research/lab/new_reuse/data/publications'
destination_folder = '/home/nicolaedrabcinski/research/lab/new_reuse/data/publications_unzippped'
extract_tar_gz(source_folder, destination_folder)

print("Все файлы разархивированы.")
