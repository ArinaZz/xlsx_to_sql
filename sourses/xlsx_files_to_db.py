"""Модуль производит чтение .xlsx файлов и их последующую запись в БД"""

from pathlib import Path
import sqlite3 as sq
import os.path

import pandas as pd

#from sourses import column_maker
import sourses.column_maker


def get_xlsx_files(data_dir):
    """Метод возвращает список путей к файлам формата .xlsx в указанной директории """

    xlsx_files = list(Path(data_dir).glob("*.xlsx"))
    return xlsx_files


def read_xlsx_files(xlsx_files_list, columns):
    """Метод производит чтение .xlsx файлов из списка путей к ним, с последующей записью всех DataFrame в единый
    список """

    cur_path = ""
    combined_dfs = []

    try:
        for files_path in xlsx_files_list:
            cur_path = files_path
            df = pd.read_excel(files_path, skiprows=8).reset_index(drop=True)

            if df.empty:
                print(f"Пустой DataFrame для файла: {str(files_path.name)[:-5]}")
                continue

            df.columns = columns
            df = df.dropna(subset=['Код проекта', 'Наименование инвестиционного проекта'])

            # Проверка на наличие пустых столбцов
            # df = df.dropna(axis=1, how='all')

            combined_dfs.append(df)

    except FileNotFoundError as e1:
        print(f'Файл не найден: {str(cur_path.name)[:-5]}')
    except PermissionError as e1:
        print(f'Нет доступа к файлу: {str(cur_path.name)[:-5]}')

    return combined_dfs


def xlsx_files_to_db():
    """Метод создает в корневой папке БД, получает названия столбцов из column_maker() и список с DataFrame из
    read_xlsx_files() и записывает все в БД """

    current_dir = os.path.abspath(os.path.dirname(__file__))  # Получаем абсолютный путь к текущей директории скрипта
    data_dir = os.path.join(os.path.dirname(current_dir), "data")  # Получаем абсолютный путь к папке 'data'
    db_path = os.path.join(os.path.dirname(current_dir), "Financing_reports.db")

    columns = sourses.column_maker.column_maker()

    con = None
    try:
        xlsx_files_list = get_xlsx_files(data_dir)
        if xlsx_files_list:
            combined_dfs = read_xlsx_files(xlsx_files_list, columns)

            if combined_dfs:
                con = sq.connect(db_path)
                con.row_factory = sq.Row

                combined_df = pd.concat(combined_dfs, ignore_index=True)
                table_name = 'Объединенная таблица'
                with con:
                    combined_df.to_sql(table_name, con, if_exists='replace', index_label='key')
        else:
            print("Файлов в указанной папке не найдено!")

    except sq.Error as e:
        if con:
            con.rollback()
        print("Ошибка выполения запроса: " + str(e))
    finally:
        if con:
            con.close()
