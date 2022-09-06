import pyodbc
import pandas
from datetime import datetime
from functools import wraps

class MSSQLCtrl:
    # 初始化
    def __init__(self, host , port, user, password,database):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database

    # 執行SQL
    def executeSQL(self,sql):
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + self.__host + "," + self.__port + ";DATABASE=" + self.__database + ";UID=" + self.__user + ";PWD=" + self.__password)
        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()
        conn.close()

    # 查詢SQL，回傳Dataframe
    def searchSQL(self, sql):
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + self.__host + "," + self.__port + ";DATABASE=" + self.__database + ";UID=" + self.__user + ";PWD=" + self.__password)
        df = pandas.read_sql(sql,conn)
        conn.close
        return df

