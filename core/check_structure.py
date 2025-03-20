import os


def list_files(startpath, target_dirs):
    for root, dirs, files in os.walk(startpath):
        # Lấy tên thư mục hiện tại
        current_dir = os.path.basename(root)
        # Kiểm tra nếu thư mục hiện tại nằm trong danh sách target_dirs
        if current_dir in target_dirs:
            level = root.replace(startpath, "").count(os.sep)
            indent = " " * 4 * (level)
            print("{}{}/".format(indent, os.path.basename(root)))
            subindent = " " * 4 * (level + 1)
            for f in files:
                print("{}{}".format(subindent, f))


# Đường dẫn tới thư mục gốc của dự án
project_root = r"f:\VSCode\PySpark Tutorial"
# Danh sách các thư mục cần lấy
target_dirs = ["core", "DataFrameExercise"]
list_files(project_root, target_dirs)
