from pathlib import Path
import sqlite3 as sq
import pandas as pd
import pathlib
from sourses.column_maker import column_maker


# Метод возвращает список путей в формате list к файлам формата .xlsx
def get_xlsx_files():
    path = str(pathlib.Path.cwd().parent) + r'\data'
    folder_path = Path(path)

    xlsx_files = list(folder_path.glob("*.xlsx"))

    return xlsx_files


# Метод создает в корневой папке БД, получает названия столбцов из column_maker(), читает файлы .xlsx и записывает все в БД
def xlsx_files_to_db():
    xlsx_files_list = get_xlsx_files()
    columns = column_maker()

    con = None
    try:
        con = sq.connect(str(pathlib.Path.cwd().parent) + r'\Financing_reports.db')
        con.row_factory = sq.Row

        try:
            for files_path in xlsx_files_list:
                df = pd.read_excel(files_path).iloc[8:].reset_index(drop=True)
                df.columns = columns
                df = df.dropna(subset='Код проекта' or 'Наименование инвестиционного проекта')

                # Проверка на наличие пустых столбцов
                # df = df.dropna(axis=1, how='all')

                table_name = 'Таблица ' + str(files_path.name)[:-5]
                with con:
                    df.to_sql(table_name, con, if_exists='replace', index_label='key')
                # print(df)
        except FileNotFoundError as e1:
            print('Файл не найден')
        except PermissionError as e2:
            print('Нет доступа к файлу')

        #con.commit()
    except sq.Error as e:
        if con:
            con.rollback()
        print("Ошибка выполения запроса: " + str(e))
    finally:
        if con:
            con.close()
