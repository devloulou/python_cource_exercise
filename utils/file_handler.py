import pandas as pd
import os

from params import FOLDER_PATH


class FileHandler:

    def get_file_list(self):
        return [item for item in os.listdir(FOLDER_PATH) if item.endswith('.csv')]

    def get_data_from_csv(self, file_name):
        return pd.read_csv(os.path.join(FOLDER_PATH, file_name))


if __name__ == '__main__':
    test = FileHandler()

    for item in test.get_file_list():
        data = test.get_data_from_csv(item)
        print(data)
        break

