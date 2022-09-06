import os , datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.sqlTool import SqlTool
from dotenv import load_dotenv
import time
import datetime
import openpyxl
import pandas as pd
import calendar

load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/GreenPlum.env")

greenplumCtrl = PostgresCtrl(
    host=os.getenv("GREENPLUS_HOST")
    , port=int(os.getenv("GREENPLUS_PORT"))
    , user=os.getenv("GREENPLUS_USER")
    , password=os.getenv("GREENPLUS_PASSWORD")
    , database="bureport"
    , schema="schemaName"
)


sqlTool = SqlTool()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', lambda x: '%.0f' % x)


def main():
    gameNameArr = ["lineage"]
    # gameNameArr = ["lineage", "maple", "tdn", "mabi", "cso", "els", "wod", "bnb", "kr"]
    schemaNameArr = []
    tableNumberArr = []
    viewMapArr = {
        "lineage": ["lineage", "lineagef2p", "lineageglobal"]
        , "maple": ["maple"]
        , "tdn": ["tdn"]
        , "mabi": ["mabi"]
        , "cso": ["cso"]
        , "els": ["els"]
        , "wod": ["wod"]
        , "bnb": ["bnb"]
        , "kr": ["kr"]
    }

    createViewSQLInit, selectDataSQLInit = CreateGPView()

    for gameName in gameNameArr:
        viewMap = [viewMap for viewMap in viewMapArr[gameName]]
        createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS [:SchemaNameView]".replace("[:SchemaNameView]", f"{gameName}_v")
        greenplumCtrl.executeSQL(createSchemaSQL)
        dropViewSQLInit = "DROP VIEW IF EXISTS [:ViewName];"

        if len(viewMap) == 1:
            searchSQL = """
                SELECT *
                FROM information_schema.tables
                where table_schema = '{}' AND table_name like 'bu%'
            """.format(gameName)
            tableDF = greenplumCtrl.searchSQL(searchSQL)

            for tableInfoIndex, tableInfoRow in tableDF.iterrows():
                tableNumber = tableInfoRow["table_name"].replace("bu", "")
                tableName = "bu"+tableNumber
                viewName = "bu"+tableNumber

                createViewSQL = createViewSQLInit.replace("[:SchemaNameView]", f"{gameName}_v").replace("[:ViewName]", viewName)
                createViewSQL += selectDataSQLInit.replace("[:SchemaName]", gameName).replace("[:TableName]", tableName)
                createViewSQL += ";"
                # print(createViewSQL)
                greenplumCtrl.executeSQL(dropViewSQLInit.replace("[:ViewName]", viewName))
                greenplumCtrl.executeSQL(createViewSQL)
                print(f"Create {gameName} View Succeeded")
        else:
            searchSQL = """
                            SELECT *
                            FROM information_schema.tables
                            where table_schema = '{}' AND table_name like 'bu%'
                        """.format(gameName)
            tableDF = greenplumCtrl.searchSQL(searchSQL)

            for tableInfoIndex, tableInfoRow in tableDF.iterrows():
                tableNumber = tableInfoRow["table_name"].replace("bu", "")
                tableName = "bu" + tableNumber
                viewName = "bu" + tableNumber

                createViewSQL = createViewSQLInit.replace("[:SchemaNameView]", f"{gameName}_v").replace("[:ViewName]", viewName)
                cnt = 1
                for schema in viewMap:
                    createViewSQL += selectDataSQLInit.replace("[:SchemaName]", schema).replace("[:TableName]", tableName)
                    if cnt < len(viewMap):
                        createViewSQL += "UNION ALL"
                        cnt += 1
                createViewSQL += ";"
                # print(createViewSQL)
                greenplumCtrl.executeSQL(dropViewSQLInit.replace("[:ViewName]", viewName))
                greenplumCtrl.executeSQL(createViewSQL)
                print(f"Create {gameName} View Succeeded")


def CreateGPView():
    createViewSQLInitCode = """
        CREATE OR REPLACE VIEW [:SchemaNameView].[:ViewName] AS """
    selectDataSQLInitCode = """
            SELECT *
            FROM [:SchemaName].[:TableName] """
    return createViewSQLInitCode, selectDataSQLInitCode


if __name__ == "__main__":
    main()
