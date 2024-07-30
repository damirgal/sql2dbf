# -*- coding: utf-8 -*-
import cx_Oracle
import sys
from dbfpy3 import dbf
import datetime
import time
import logging

# настройки
sql_dir = 'c:/sql/'   # каталог с sql-файлами, пример: 'c:/sql/'
dbf_dir = 'c:/dbf/'     # каталог для выгрузки DBF, пример: 'c:/dbf/'
log_dir = 'c:/log/'       # каталог для лог-файлов, пример: 'c:/log/'


# запуск файла:
#python c:/путь/к/python/файлу.py c:/путь/dbf_файл.sql win|dbf

# время запуска скрипта
st = time.time()

# логирование
log_name = log_dir + 'sql2dbf_' + str(datetime.date.today()) + '.log'
logging.basicConfig(filename=log_name, format='%(levelname)s -> %(asctime)s: %(message)s', level=logging.DEBUG)
logging.info(f"Начало работы: {sys.argv[1]}")


try:
    # чтение аргументов
    sql_name = sql_dir + sys.argv[1]
    file_name = dbf_dir + sys.argv[1][0:sys.argv[1].rfind('.')]+'.dbf'

    
    # выбор кодировки, третий аргумент, необязателен, по умолчанию кодировка win
    if len(sys.argv)==3:
        if sys.argv[2] == 'dos':
            encod = 0x26
        elif sys.argv[2] == 'win':
            encod = 0xc9
    else:
        encod = 0xc9
            

    # Параметры подключения к БД
    dsn_tns = cx_Oracle.makedsn('', '1521', '192.168.1.100')
    conn = cx_Oracle.connect(user='user', password='pass', dsn='')

    def execute_and_return_dict(conn, sql):
        cur = conn.cursor()
        cur.execute(sql)
        columns = [col[0] for col in cur.description]
        cur.rowfactory = lambda *args: dict(zip(columns, args))
        return cur.fetchall()

    def rec1(w, w1, row):
        for j in range(len(w)):
            rec[w1[j]] = q[row][w[j]]
        return rec

    # чтение sql-файла
    f = open(sql_name)
    sql = f.read()
    q = execute_and_return_dict(conn, sql)

    # описание полей dbf
    # 0xc9 cp1251 Russian Windows
    # 0x26 cp866 Russian OEM
    # 0x00 Default?
    db = dbf.Dbf(file_name, new=True)
    db.header.code_page = encod
    w = []      # список полей вида: ПОЛЕ_ТИП_ДЛИНА (пример: ID_N_9)
    w1 = []     # список полей вида: ПОЛЕ (пример: ID)

    # создаем схему данных для всех столбцов, вида: ("ID", "N", 9)
    for i in range(len(list(q[0]))):
        db.add_field((
                        # список типов полей
                        list(q[0].keys())[i][list(q[0].keys())[i].rfind('_')-1:list(q[0].keys())[i].rfind('_')],
                        # список полей
                        list(q[0].keys())[i][0:list(q[0].keys())[i].rfind('_')-2],
                        # длина полей
                        list(q[0].keys())[i][list(q[0].keys())[i].rfind('_')+1:]
                        ))
        
        w.append(list(q[0].keys())[i][0:])
        w1.append(list(q[0].keys())[i][0:list(q[0].keys())[i].rfind('_')-2])
        

    # формирование dbf файла    
    for row in range(len(list(q))):
        rec = db.new()      # добавление новой строки
        rec1(w, w1, row)    # функция собирает и возвращает строку для добавления в DBF
        db.write(rec)       # записывает строку
    db.close()

    conn.close()


    et = time.time()
    elapsed_time = et - st
    #print(f"Файл {file_name} выгружен, строк {row+1}, время выполнения {elapsed_time:.2f} сек.")
    logging.info(f"Файл {file_name} выгружен, строк {row+1}, за {elapsed_time:.2f} сек.")


except Exception as e:
    print("Произошла ошибка:", e)
    logging.error(e)
