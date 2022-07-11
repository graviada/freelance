import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
# Импортируем модуль sys для наиболее ясного понимания природы возможных ошибок на Python
import sys

import pandas as pd
from math import isnan


# Определим две функции для отслеживания ошибок в psycopg2
def show_psycopg2_exception(err):
    # Получаем детали об ошибке
    err_type, err_obj, traceback = sys.exc_info()
    # Получаем номер строки, в котором появилась ошибка
    line_n = traceback.tb_lineno

    # Печатаем возникшую ошибку
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    print("\nextensions.Diagnostics:", err.diag)
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successfully..................")

    except OperationalError as err:
        # Передаем ошибку в функцию
        show_psycopg2_exception(err)
        # Устанавливаем соединение в 'None' при появлении такой ошибки
        conn = None
    return conn


# Определим функцию для вставки датафрейма с помощью psycopg2.extras.execute_values()
def execute_values(conn, df, table):
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in df.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(df.columns))

    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        conn.commit()
        print("Data inserted using execute_values() successfully..")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()


def create_region_table(reestr):
    region = reestr[["dev_region_code", "region_name"]]
    region = region.rename(columns={"dev_region_code": "region_code"})

    is_found = {region.region_code[0]: region.region_name[0]}
    for i in range(len(region.region_code)):
        code = region.region_code[i]
        name = region.region_name[i]
        is_found[code] = name

    is_found = {k: is_found[k] for k in is_found if not isnan(k)}
    region = pd.DataFrame(is_found.items(), columns=["region_code", "region_name"])

    return region


DB_HOST = "localhost"
DB_NAME = "dom.rf_db"
DB_USER = "postgres"
DB_PASS = "1234"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

# Создаем таблицы
cur = conn.cursor()
# Таблица с регионами
cur.execute("""CREATE TABLE region (
  region_code SMALLINT PRIMARY KEY,
  region_name VARCHAR(40) NOT NULL UNIQUE
);
""")
conn.commit()

# Таблица реестра
cur.execute("""CREATE TABLE reestr (
  dev_id INT PRIMARY KEY,
  dev_short_name VARCHAR(150) NOT NULL,
  build_obj SMALLINT,
  comiss_obj SMALLINT,
  dev_site VARCHAR(150) NOT NULL,
  dev_fact_addr TEXT NULL,
  dev_region_code SMALLINT NULL,
  FOREIGN KEY (dev_region_code) REFERENCES region(region_code)
);
""")
conn.commit()

# Таблица новостроек
cur.execute("""CREATE TABLE new_building (
  obj_id INT PRIMARY KEY,
  dev_id INT NOT NULL,
  obj_addr TEXT,
  obj_floor_min SMALLINT,
  obj_floor_max SMALLINT,
  obj_completion_date DATE,
  objSquareLiving FLOAT(2),
  latitude FLOAT(4),
  longitude FLOAT(4),
  build_type VARCHAR(10) NOT NULL,
  FOREIGN KEY (dev_id) REFERENCES reestr(dev_id)
);
""")
conn.commit()

# Заполняем таблицы данными
reestr = pd.read_excel("dom.rf/reestr_dom.rf.xlsx",
                       names=["dev_id", "dev_short_name", "build_obj", "comiss_obj",
                              "region_name", "dev_site", "dev_fact_addr", "dev_region_code"])

# Заполняем таблицу с регионами
region = create_region_table(reestr=reestr)
execute_values(conn, region, "region")

# Выполняем предобработку и заполняем таблицу с застройщиками
reestr = reestr.drop("region_name", axis=1)
reestr = reestr.fillna(0)

# cur.execute("""INSERT INTO region("region_code", "region_name") VALUES (0, 'Не указан')""")
# conn.commit()

reestr = reestr.drop_duplicates(subset=["dev_id"])
execute_values(conn, reestr, "reestr")

new_buildings = pd.read_excel("dom.rf/new_buildings_dom.rf.xlsx",
                              names=["obj_id", "dev_id", "obj_addr",
                                     "obj_floor_min", "obj_floor_max",
                                     "obj_completion_date", "objSquareLiving",
                                     "latitude", "longitude", "build_type"])

# Предобработка и загрузка данных в таблицу новостроек
new_buildings = new_buildings.fillna(0)
# print(new_buildings.isna().any())
# print(new_buildings.columns[0])

# length = len(new_buildings.dev_id)
# print(f'{length} \n {new_buildings.loc[new_buildings.duplicated(), :]}')
# for col in new_buildings.columns:
#     print(f'{col}: {new_buildings[col].duplicated().any()}')
new_buildings.drop_duplicates(inplace=True, subset=["obj_id"])

some = new_buildings[new_buildings.dev_id == 11024].index
new_buildings.drop(some, inplace=True)

execute_values(conn, new_buildings, "new_building")