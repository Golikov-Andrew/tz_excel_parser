import sqlite3
import re
import jinja2

from copy import deepcopy
from datetime import datetime

import pandas as pd


def to_excel_file(data: list, dest_filepath: str):
    df = pd.DataFrame(data)
    df.style.set_table_styles(
        [{
            'selector': 'th',
            'props': [
                ('background-color', 'black'),
                ('color', 'cyan')]
        }])
    df.to_excel(dest_filepath, index=False)


def to_db_table(data: list, db_name: str, table_name: str):
    with sqlite3.connect(f'{db_name}.db') as con:
        cur = con.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        columns = list()
        specs = list()
        for k in data[0].keys():
            columns.append(k)
            specs.append(f':{k}')

        query = f"CREATE TABLE {table_name}({', '.join(columns)});"
        cur.execute(query)
        cur.executemany(f"INSERT INTO {table_name} VALUES({', '.join(specs)})", parsed_data)
        con.commit()


class ExcelFileParser:
    __COLUMN_SEPARATOR = '---|---'

    def __init__(self):
        self.__dates = dict()
        self.__target_attrs = list()
        self.__df = None

    def __prepare_column(self, col):
        if isinstance(col, tuple):
            col = self.__COLUMN_SEPARATOR.join(str(c) for c in col)
        for k, v in self.__dates.items():
            col = col.replace(k, v)
        return col

    def __init_columns(self):
        self.__df.columns = map(self.__prepare_column, self.__df.columns)
        columns_names = list(self.__df.columns.values)
        map_for_rename_columns = dict()
        for column_name in columns_names:
            new_name = list()
            parts = column_name.split(self.__COLUMN_SEPARATOR)
            for part in parts:
                if re.match(r'Unnamed: [\d]+_level_[\d]+', part) is not None:
                    break
                new_name.append(part)
            map_for_rename_columns[column_name] = '__'.join(new_name)
        self.__df.rename(columns=map_for_rename_columns, inplace=True)

    def parse_data(self, src_excel_filepath: str, target_attrs: list, dates: dict):
        self.__dates = dates
        self.__target_attrs = target_attrs
        self.__df = pd.read_excel(src_excel_filepath, header=[0, 1, 2])
        self.__init_columns()
        records = self.__df.to_dict('records')
        result_list = list()
        for record in records:
            new_record = deepcopy(record)
            result_list.append(new_record)
            for k, v in record.items():
                for attr in self.__target_attrs:
                    if attr in k:
                        for date in self.__dates.values():
                            if date in k:
                                key = f'total__{attr}__{date}'
                                if key not in new_record:
                                    new_record[key] = 0
                                new_record[key] += v
        return result_list


if __name__ == '__main__':
    table_name = 'target_table'

    excel_file_parser = ExcelFileParser()
    parsed_data = excel_file_parser.parse_data(
        src_excel_filepath='Приложение к заданию бек разработчика.xlsx',
        target_attrs=['Qliq', 'Qoil'],
        dates={
            'data1': datetime(2023, 1, 10).strftime('%Y_%m_%d'),
            'data2': datetime(2023, 1, 20).strftime('%Y_%m_%d')
        }
    )
    to_excel_file(
        parsed_data,
        dest_filepath=f'{table_name}.xlsx'
    )
    to_db_table(
        parsed_data,
        db_name='tz_db',
        table_name=table_name
    )
