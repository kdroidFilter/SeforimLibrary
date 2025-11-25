import os
import glob

def add_txt_extension_to_files(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if '.' not in file:
                file_path = os.path.join(root, file)
                new_file_path = os.path.join(root, file + '.txt')
                os.rename(file_path, new_file_path)
        for dir in dirs:
            add_txt_extension_to_files(os.path.join(root, dir))

add_txt_extension_to_files('אוצריא')