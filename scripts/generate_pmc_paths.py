import os

cur_dir = os.getcwd()

pubs_dir = "/home/nicolaedrabcinski/research/lab/new_reuse/data/publications_unzippped"
path_file = "/home/nicolaedrabcinski/research/lab/new_reuse/data/pmc_paths.txt"

xml_paths = []

# Проходимся по всем субдиректориям и файлам в указанной директории
for root, dirs, files in os.walk(pubs_dir):
    for file in files:
        # Проверяем, что файл имеет расширение .xml
        if file.endswith(".xml"):
            # Формируем полный путь к файлу и добавляем его в список
            xml_path = os.path.join(root, file)
            xml_paths.append(xml_path)

# Сохраняем пути к файлам формата .xml в файл
with open(path_file, "w") as f:
    for path in xml_paths:
        f.write(path + "\n")