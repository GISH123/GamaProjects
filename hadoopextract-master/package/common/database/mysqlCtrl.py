
import pymysql
import pandas

class MySQLCtrl:
    # 初始化
    def __init__(self, host=None, port=None , user=None, password=None, database=None):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database

    # 執行SQL
    def executeSQL(self, sql):
        conn = pymysql.connect(host=self.__host, port=self.__port, user=self.__user, password=self.__password, db=self.__database)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.close

    # 查詢SQL，回傳Dataframe
    def searchSQL(self, sql):
        conn = pymysql.connect(host=self.__host, port=self.__port, user=self.__user, password=self.__password, db=self.__database)
        df = pandas.read_sql(sql, conn)
        conn.close
        return df

    #----------------------------------------------------------------------------------------------------


