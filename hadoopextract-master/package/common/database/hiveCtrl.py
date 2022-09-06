from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import time

class HiveCtrl:
    # 初始化
    def __init__(self, host=None, port=None , user=None, password=None, database=None,auth_mechanism="PLAIN"):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database
        self.__auth_mechanism = auth_mechanism

    # 執行SQL
    def executeSQL(self, sql ):
        conn = connect(host=self.__host, port=self.__port, user=self.__user, password=self.__password
                       , database=self.__database,auth_mechanism= self.__auth_mechanism)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.close

    # 查詢SQL，回傳Dataframe
    def searchSQL(self, sql):
        conn = connect(host=self.__host, port=self.__port, user=self.__user, password=self.__password
                       , database=self.__database, auth_mechanism=self.__auth_mechanism)
        cursor = conn.cursor()
        cursor.execute(sql)
        df = as_pandas(cursor)
        return df

    #----------------------------------------------------------------------------------------------------
	# 解決Hadoop概率性錯誤問題
    def executeSQL_TCByCount(self, sql,TCCount,printError=False):
        excount = 1
        while 1 <= excount and excount <= TCCount:
            try:
                time.sleep((excount - 1) * 10)
                return self.executeSQL(sql)
            except Exception as e:
                excount = excount + 1
                print("Error SQL excount: {} \n".format(str(excount)))
                print(e if printError == True else "")
                print(sql if printError == True else "")
        print('info: Fail to execute SQL! \n'+sql)

    def searchSQL_TCByCount(self, sql,TCCount,printError=False):
        excount = 1
        while 1 <= excount and excount <= TCCount:
            try:
                time.sleep((excount - 1) * 10)
                return self.searchSQL(sql)
            except:
                excount = excount + 1
                print("Error SQL excount: {} \n".format(str(excount)))
                print(e if printError == True else "")
                print("sql" if printError == True else "")
        print('info: Fail to search SQL! \n'+sql)
