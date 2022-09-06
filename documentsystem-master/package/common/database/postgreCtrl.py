import io
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import re
class PostgresCtrl:
    # 初始化
    def __init__(self, host=None, port=None, user=None, password=None,database=None,schema=None):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database
        self.__schema = schema

    # 確認資料表是否存在
    def checkTable(self, table_name):
        conn = psycopg2.connect(database=self.__database, user=self.__user, password=self.__password,
                                host=self.__host, port=self.__port)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM information_schema.tables where table_schema = '" + self.__schema + "' AND table_name = '" + table_name + "'")
        is_exist = False
        if cur.rowcount > 0:
            is_exist = True
        else:
            is_exist = False
        conn.close
        return is_exist

    # 取得資料表資訊
    def getTableInfo(self, tableName=None):
        conn = psycopg2.connect(database=self.__database, user=self.__user, password=self.__password, host=self.__host,port=self.__port)
        schemaName = self.__schema
        if tableName.find(".") >= 0 :
            schemaName = tableName.split(".")[0]
            tableName = tableName.split(".")[1]

        sql = """
            SELECT 
                AA.table_schema as tableschema
                , AA.table_name as tablename 
                , AA.ordinal_position as position
                , AA.column_name as columnname
                , AA.data_type as datatype
                , CASE 
                    WHEN AA.data_type like '%timestamp%' THEN 'Time'
                    WHEN AA.data_type like '%int%' THEN 'Integer'
                    WHEN AA.data_type like '%double%' THEN 'Double'
                    ELSE 'String' 
                  END as inserttype
            FROM information_schema.columns AA
            WHERE 1 = 1 
                AND AA.table_schema = '[:SchemaName]'
                AND AA.table_name = '[:TableName]'
                AND AA.column_name not like '%_pk'
            ORDER BY 
                ordinal_position ;
        """.replace("[:SchemaName]",schemaName).replace("[:TableName]",tableName)
        df = pd.read_sql(sql, conn)
        conn.close
        return df

    # 執行SQL
    def executeSQL(self, sql):
        conn = psycopg2.connect(database=self.__database, user=self.__user, password=self.__password, host=self.__host, port=self.__port)
        cur = conn.cursor()
        cur.execute(sql)
        cur.execute("commit;")
        conn.close

    # 查詢SQL，回傳Dataframe格式的資料
    def searchSQL(self, sql):
        conn = psycopg2.connect(database=self.__database, user=self.__user, password=self.__password, host=self.__host, port=self.__port)
        df = pd.read_sql(sql, conn)
        conn.close
        return df

    # 將Dataframe，使用語法方式，直接塞入資料庫指定Table
    def insertData(self, tableName=None, insertTableInfoDF=None , insertDataDF=None,insertMaxCount=200):
        conn = psycopg2.connect(database=self.__database, user=self.__user, password=self.__password, host=self.__host,port=self.__port)
        cur = conn.cursor()
        schemaName = self.__schema
        if tableName.find(".") >= 0:
            schemaName = tableName.split(".")[0]
            tableName = tableName.split(".")[1]

        insertCodeInitStr = "INSERT INTO [:TableName] ( [:Columns] ) VALUES [:Values] "
        columnsStr = ""
        valuesListStr = ""
        for infoIndex, infoRow in insertTableInfoDF.iterrows():
            if columnsStr == "":
                columnsStr = infoRow["columnname"]
            else:
                columnsStr = columnsStr + "," + infoRow["columnname"]

        insertCount = 0
        for dataIndex, dataRow in insertDataDF.iterrows():
            valuesStr = ""
            for infoIndex, infoRow in insertTableInfoDF.iterrows():
                columnName = infoRow['columnname']
                insertType = infoRow['inserttype']
                valueStr = ""
                if valuesStr != "":
                    valueStr = ","
                if (insertType == "String" or insertType == "Date" or insertType == "Time") and dataRow[columnName] != None and pd.isnull(dataRow[columnName]) == False:
                    valueStr = valueStr + "'{}'"
                else:
                    valueStr = valueStr + "{}"
                if dataRow[columnName] == None or pd.isnull(dataRow[columnName]) :
                    dataValueStr = "null"
                else:
                    dataValueStr = str(dataRow[columnName])
                valuesStr = valuesStr + valueStr.format(dataValueStr.replace("'","''"))
            if valuesListStr == "":
                valuesStr = "(" + valuesStr + ")"
            else:
                valuesStr = ",(" + valuesStr + ")"
            valuesListStr = valuesListStr + "\n" + valuesStr
            insertCount = insertCount + 1
            if insertCount >= insertMaxCount:
                insertCodeStr = insertCodeInitStr.replace("[:TableName]", "{}.{}".format(schemaName,tableName))
                insertCodeStr = insertCodeStr.replace("[:Columns]", columnsStr)
                insertCodeStr = insertCodeStr.replace("[:Values]", valuesListStr)
                cur.execute(insertCodeStr)
                print("已寫入 {} {} 筆資料".format(tableName,str(insertCount)))
                valuesListStr = ""
                insertCount = 0

        if insertCount > 0:
            insertCodeStr = insertCodeInitStr.replace("[:TableName]", "{}.{}".format(schemaName,tableName))
            insertCodeStr = insertCodeStr.replace("[:Columns]", columnsStr)
            insertCodeStr = insertCodeStr.replace("[:Values]", valuesListStr)
            cur.execute(insertCodeStr)
            print("已寫入 {} {} 筆資料".format(tableName, str(insertCount)))
        cur.execute("commit;")
        conn.close

    # 將Dataframe，使用IO方式，直接塞入資料庫指定Table
    def writeDFToTable(self, df, table_name, if_exists="fail", float_format=None):
        db_engine = create_engine("postgresql://" + self.__user + ":" + self.__password + "@" + self.__host + ":" + str(
            self.__port) + "/" + self.__database)
        string_data_io = io.StringIO()
        re_null = re.compile(pattern='\x00')
        df.replace(regex=re_null, value=' ', inplace=True)
        if float_format == None:
            df.to_csv(string_data_io, sep="|", index=False)
        else:
            df.to_csv(string_data_io, sep="|", index=False, float_format=float_format)
        pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
        table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df, index=False, if_exists=if_exists,
                                   schema=self.__schema)
        table.create()
        string_data_io.seek(0)
        # string_data_io.readline()
        with db_engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                copy_cmd = "COPY " + self.__schema + ".%s FROM STDIN HEADER DELIMITER '|' CSV" % table_name
                cursor.copy_expert(copy_cmd, string_data_io)
            connection.connection.commit()