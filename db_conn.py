import psycopg2
from datetime import datetime


def db_connection():
    connection = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow",
                                  port='5432')
    cursor = connection.cursor()
    cursor.execute("select * from information_schema.tables where table_name=%s", ('dateTimeInfo',))
    res = bool(cursor.rowcount)
    print(res)
    if res:
        create_table = """CREATE TABLE dateTimeInfo(
            DATETIME VARCHAR(20));"""
        cursor.execute(create_table)
        print("Table created.")
    else:
        print("Table already exist.")

    insert = """INSERT INTO dateTimeInfo(DATETIME) VALUES(%s);"""
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    cursor.execute(insert, [dt_string])

    show = """SELECT * FROM dateTimeInfo;"""
    cursor.execute(show)
    data = cursor.fetchall()
    print(data)
    connection.commit()
    connection.close()
