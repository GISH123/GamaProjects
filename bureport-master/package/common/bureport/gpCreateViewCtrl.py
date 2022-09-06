import os, time
from package.common.database.postgreCtrl import PostgresCtrl
'''
# 第一個為View的SchemaName，若無多產品可為空
"viewSchemaMapArr": ['lineage', 'lineagef2p', 'lineageglobal']
'''

class GPCreateViewCtrl:
    
    def CreateView(self, makeInfo):
        gameName = makeInfo["gameName"]
        viewSchemaMapArr = makeInfo["viewSchemaMapArr"]
        if viewSchemaMapArr == []:
            viewSchemaMapArr = [gameName]
        # print(viewSchemaMapArr)
        createViewSQLInit, selectDataSQLInit = self.CreateViewSql()

        greenplumCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="bureport"
            , schema="schemaName"
        )

        createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS [:SchemaNameView]".replace("[:SchemaNameView]", f"{viewSchemaMapArr[0]}_v")
        greenplumCtrl.executeSQL(createSchemaSQL)
        dropViewSQLInit = "DROP VIEW IF EXISTS [:SchemaNameView].[:ViewName];"

        startRunTime = time.time()
        if len(viewSchemaMapArr) <= 1:
            searchSQL = """
                SELECT DISTINCT table_name 
                FROM information_schema.tables
                where table_schema in ('{}') 
                AND table_name like 'bu%'
            """.format(gameName)
            tableDF = greenplumCtrl.searchSQL(searchSQL)

            for tableInfoIndex, tableInfoRow in tableDF.iterrows():
                tableNumber = tableInfoRow["table_name"].replace("bu", "")
                tableName = "bu" + tableNumber
                viewName = "bu" + tableNumber

                createViewSQL = createViewSQLInit.replace("[:SchemaNameView]", f"{gameName}_v").replace("[:ViewName]", viewName)
                createViewSQL += selectDataSQLInit.replace("[:SchemaName]", gameName).replace("[:TableName]", tableName)
                createViewSQL += ";"
                # print(createViewSQL)
                # greenplumCtrl.executeSQL(dropViewSQLInit.replace("[:SchemaNameView]", f"{gameName}_v").replace("[:ViewName]", viewName))
                greenplumCtrl.executeSQL(createViewSQL)
            print(f"Create {gameName}_v View Succeeded, Used {time.time() - startRunTime} seconds.")
        else:
            # 組合所有Schema
            allGameName = ''
            schemaCnt = len(viewSchemaMapArr)
            for dbSchema in viewSchemaMapArr:
                if schemaCnt > 1:
                    allGameName += dbSchema
                    allGameName += "','"
                    schemaCnt -= 1
                else:
                    allGameName += dbSchema
            searchSQL = """
                SELECT DISTINCT table_name 
                FROM information_schema.tables
                where table_schema in ('{}') 
                AND table_name like 'bu%'
                """.format(allGameName)
            tableDF = greenplumCtrl.searchSQL(searchSQL)

            for tableInfoIndex, tableInfoRow in tableDF.iterrows():
                tableNumber = tableInfoRow["table_name"].replace("bu", "")
                tableName = "bu" + tableNumber
                viewName = "bu" + tableNumber

                createViewSQL = createViewSQLInit.replace("[:SchemaNameView]", f"{viewSchemaMapArr[0]}_v").replace("[:ViewName]", viewName)
                cnt = 1
                # 撈取擁有Table的Schema，避免因table不存在造成建立View失敗
                schemaSQL = """
                    SELECT DISTINCT table_schema 
                    FROM information_schema.tables
                    where table_schema in ('{}') 
                    AND table_name = '{}'
                    """.format(allGameName, tableName)
                schemaDF = greenplumCtrl.searchSQL(schemaSQL)

                for schemaInfoIndex, schemaInfoRow in schemaDF.iterrows():
                    schema = schemaInfoRow["table_schema"]
                    createViewSQL += selectDataSQLInit.replace("[:SchemaName]", schema).replace("[:TableName]", tableName)
                    if cnt < len(schemaDF):
                        createViewSQL += "UNION ALL"
                        cnt += 1
                createViewSQL += ";"
                # print(createViewSQL)
                # greenplumCtrl.executeSQL(dropViewSQLInit.replace("[:SchemaNameView]", f"{viewSchemaMapArr[0]}_v").replace("[:ViewName]", viewName))
                greenplumCtrl.executeSQL(createViewSQL)
            print(f"Create {viewSchemaMapArr[0]}_v View Succeeded, Used {time.time() - startRunTime} seconds.")

    def CreateViewSql(self):
        createViewSQLInitCode = """
            CREATE OR REPLACE VIEW [:SchemaNameView].[:ViewName] AS """
        selectDataSQLInitCode = """
                SELECT *
                FROM [:SchemaName].[:TableName] """
        return createViewSQLInitCode, selectDataSQLInitCode