# -*- coding: utf-8 -*-
import cx_Oracle
import sys
from dbfpy3 import dbf

# запуск файла:
# 1. прописать подключение к БД
# 2. python c:/путь/к/python/файлу.py c:/путь/dbf_файл.sql win|dbf

# чтение аргументов
#sql_name = sys.argv[1][sys.argv[1].rfind('/')+1:]
#file_name = 'путь/' + sql_name[0:sql_name.rfind('.')]+'.dbf'
sql_name = 'c:/sql2dbf/' + sys.argv[1]
file_name = 'C:/temp/' + sys.argv[1][0:sys.argv[1].rfind('.')]+'.dbf'


# выбор кодировки
if len(sys.argv)==3:
    if sys.argv[2] == 'dos':
        encod = 0x26
    elif sys.argv[2] == 'win':
        encod = 0xc9
else:
    encod = 0xc9
        

dsn_tns = cx_Oracle.makedsn('dbname', 'port', 'address')
conn = cx_Oracle.connect(user='', password='', dsn='')

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
w = []
w1 = []
for i in range(len(list(q[0]))):
    db.add_field((
                    list(q[0].keys())[i][list(q[0].keys())[i].rfind('_')-1:list(q[0].keys())[i].rfind('_')],
                    list(q[0].keys())[i][0:list(q[0].keys())[i].rfind('_')-2],
                    list(q[0].keys())[i][list(q[0].keys())[i].rfind('_')+1:]
                    ))
    #print(list(q[0].keys())[i].rfind('_')+1)
    
    w.append(list(q[0].keys())[i][0:])
    w1.append(list(q[0].keys())[i][0:list(q[0].keys())[i].rfind('_')-2])

# формирование dbf файла    
for row in range(len(list(q))):
    rec = db.new()
    rec1(w, w1, row) # функция
    db.write(rec)
db.close()


conn.close()