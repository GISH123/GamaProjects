import pandas
import time
import asyncio
import datetime
import copy

class HadoopCheckCtrl:

    def __init__(self):
        pass

    def CreateSchemaTable(self , databaseCtrl , schemaName):

        dropSchemaSQL = """
            DROP TABLE IF EXISTS [:SchemaName].datasize;
            DROP TABLE IF EXISTS [:SchemaName].datacheck;
            DROP TABLE IF EXISTS [:SchemaName].datacheckdetail;
            DROP TABLE IF EXISTS [:SchemaName].datachecknoin;
            DROP TABLE IF EXISTS [:SchemaName].datacheckin;
            DROP SCHEMA IF EXISTS [:SchemaName];
        """.replace("[:SchemaName]",schemaName)

        createSchemaSQL = """
            CREATE SCHEMA IF NOT EXISTS [:SchemaName];
        """.replace("[:SchemaName]",schemaName)

        createDataSizeSQL = """
            CREATE TABLE IF NOT EXISTS [:SchemaName].datasize (
                datatime timestamp NULL,
                gamename text NULL,
                dbname text NULL,
                world text NULL,
                tablename text NULL,
                datasize int8 NULL
            );
        """.replace("[:SchemaName]",schemaName)

        createDataCheckSQL = """
            CREATE TABLE IF NOT EXISTS [:SchemaName].datacheck (
                datatime timestamp NULL,
                gamename text NULL,
                dbname text NULL,
                errorcount int4 NULL,
                errorsize int8 NULL,
                message text NULL ,
                memo text NULL 
            );
        """.replace("[:SchemaName]",schemaName)

        createDataCheckDetailSQL = """
            CREATE TABLE IF NOT EXISTS [:SchemaName].datacheckdetail (
                datatime timestamp NULL ,
                gamename text NULL ,
                dbname text NULL ,
                world text NULL ,
                tablename text NULL ,
                errorlevel text NULL ,
                datanowday int8 NULL ,
                data7day int8 NULL ,
                errorcount int4 NULL ,
                errorsize int8 NULL ,
                message text NULL ,
                memo text NULL 
            );
        """.replace("[:SchemaName]",schemaName)

        createDataCheckNoInSQL = """
            CREATE TABLE IF NOT EXISTS [:SchemaName].datachecknoin (
                gamename text NULL ,
                dbname text NULL ,
                tablename text NULL ,
                errorlevel text NULL
            );
        """.replace("[:SchemaName]",schemaName)

        createDataCheckInSQL = """
            CREATE TABLE IF NOT EXISTS [:SchemaName].datacheckin (
                gamename text NULL ,
                dbname text NULL ,
                tablename text NULL ,
                errorlevel text NULL
            );
        """.replace("[:SchemaName]",schemaName)

        databaseCtrl.executeSQL(dropSchemaSQL)
        databaseCtrl.executeSQL(createSchemaSQL)
        databaseCtrl.executeSQL(createDataSizeSQL)
        databaseCtrl.executeSQL(createDataCheckSQL)
        databaseCtrl.executeSQL(createDataCheckDetailSQL)
        databaseCtrl.executeSQL(createDataCheckNoInSQL)
        databaseCtrl.executeSQL(createDataCheckInSQL)

    def makeDataSize_HDFS(self ,makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        gameNameArr = makeInfo["gameNameArr"] if "gameNameArr" in makeInfo.keys() else []
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None
        hdfsSSHCtrl = makeInfo["hdfsSSHCtrl"] if "hdfsSSHCtrl" in makeInfo.keys() else None

        # 清除當天DB資料
        # ====================================================================================================
        for gameName in gameNameArr:
            deleteSqlStr = """
               DELETE FROM [:SchemaName].datasize
               WHERE 1 = 1
                   AND datatime >= '[:StartDate]' 
                   AND datatime < '[:EndDate]'
                   AND ('Game' = '[:GameName]' OR gamename = '[:GameName]')
            """
            deleteSqlStr = deleteSqlStr.replace("[:SchemaName]", schemaName)
            deleteSqlStr = deleteSqlStr.replace("[:GameName]", gameName)
            deleteSqlStr = deleteSqlStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
            deleteSqlStr = deleteSqlStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))

            # 新增當天DB資料
            # ====================================================================================================
            print("撈取 " + gameName + " " + makeTime.strftime("%Y%m%d") + " 當天資料")

            hadoopDataDF = pandas.DataFrame([], columns=["datatime", "gamename", "dbname", "world", "tablename", "datasize"])
            errCount = 0
            while hadoopDataDF.size == 0 and errCount < 3 :
                makeDataSizeInfo = makeInfo
                makeDataSizeInfo["gameName"] = gameName
                hadoopDataDF = self.makeDataSizeDF(makeInfo)
                errCount = errCount + 1

            if errCount != 3 :
                databaseCtrl.executeSQL(deleteSqlStr)
                tableName = "[:SchemaName].datasize".replace("[:SchemaName]", schemaName)
                tableInfoDF = databaseCtrl.getTableInfo(tableName)
                # print(hadoopDataDF)
                databaseCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=hadoopDataDF)

    def makeDataSize_GreenPlum(self, makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        gameNameArr = makeInfo["gameNameArr"] if "gameNameArr" in makeInfo.keys() else []
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None
        databaseCtrl_CheckDB = makeInfo["databaseCtrl_CheckDB"] if "databaseCtrl_CheckDB" in makeInfo.keys() else None
        mainDataList = []
        for gameName in gameNameArr:

            deleteSqlStr = """
                           DELETE FROM [:SchemaName].datasize
                           WHERE 1 = 1
                               AND datatime >= '[:StartDate]' 
                               AND datatime < '[:EndDate]'
                               AND ('Game' = '[:GameName]' OR gamename = '[:GameName]')
                        """
            deleteSqlStr = deleteSqlStr.replace("[:SchemaName]", schemaName)
            deleteSqlStr = deleteSqlStr.replace("[:GameName]", gameName)
            deleteSqlStr = deleteSqlStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
            deleteSqlStr = deleteSqlStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            databaseCtrl.executeSQL(deleteSqlStr)
            searchTableSql = """
                SELECT 
                    table_catalog
                    , table_schema
                    , table_name
                FROM information_schema.tables 
                WHERE 1 = 1 
                    AND table_schema = '[:GameName]'
                    AND (table_name like 'bu____' or table_name like 'md____') 

            """.replace("[:GameName]", gameName)
            tableDF = databaseCtrl_CheckDB.searchSQL(searchTableSql)
            searchSql = ""
            for index, row in tableDF.iterrows():
                singleSql = """                    SELECT 
                        '[:GameName]' as gamename 
                        , '[:TableName]' as tablename 
                        , world as world
                        , count(*) as datasize
                    FROM [:GameName].[:TableName] 
                    WHERE 1 = 1 
                        AND dt = '[:DateLine]' 
                    GROUP BY world
                    """
                singleSql = singleSql.replace("[:GameName]", gameName).replace("[:TableName]", row["table_name"]).replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
                searchSql = (singleSql) if searchSql == "" else (searchSql + "UNION ALL\n" + singleSql)
            if searchSql == "" :
                continue
            #print(searchSql)
            datasizeDF = databaseCtrl_CheckDB.searchSQL(searchSql)

            for index, row in datasizeDF.iterrows():
                makeDataSizeDataInfo = copy.copy(makeInfo)
                makeDataSizeDataInfo["databaseCtrl"] = None
                makeDataSizeDataInfo["databaseCtrl_CheckDB"] = None
                makeDataSizeDataInfo["rowData"] = [row["gamename"],row["world"],row["tablename"],row["datasize"]]
                mainData = eval(f"self.makeDataSizeData_{makeName}({makeDataSizeDataInfo})")
                mainDataList.append(mainData)



        mainDF = pandas.DataFrame(mainDataList, columns=["datatime", "gamename", "dbname", "world", "tablename", "datasize"])

        mainDFgroup = mainDF.groupby(by=["datatime", "gamename", "dbname", "world", "tablename"], as_index=False)[["datasize"]].sum()

        tableName = "[:SchemaName].datasize".replace("[:SchemaName]", schemaName)

        tableInfoDF = databaseCtrl.getTableInfo(tableName)
        databaseCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=mainDFgroup)


    def makeDataSizeDF(self ,makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        hdfsSSHCtrl = makeInfo["hdfsSSHCtrl"] if "hdfsSSHCtrl" in makeInfo.keys() else None

        shReplaceArr = [
            ["[:GameName]", gameName]
            , ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
            , ["[:YYMMNoLine]", makeTime.strftime("%Y%m")]
        ]
        makeDataSizeDataInfo = copy.copy(makeInfo)
        makeDataSizeDataInfo["databaseCtrl"] = None
        makeDataSizeDataInfo["hdfsSSHCtrl"] = None
        hadoopSHStrArr = eval(f"self.getHadoopSHStrArr_{makeName}({makeDataSizeDataInfo})")
        mainDataList = []
        for shHadoopStr in hadoopSHStrArr:
            for shReplace in shReplaceArr:
                shHadoopStr = shHadoopStr.replace(shReplace[0], shReplace[1])
            try:
                # print(shHadoopStr)
                hdfsSSHCtrl.reconnect()
                execRetrunStr = hdfsSSHCtrl.ssh_exec_cmd_return(shHadoopStr)
                # print(execRetrunStr)
                if execRetrunStr is not None:
                    retrunStrArr = execRetrunStr.split("\r\n")

                    for retrunStr in retrunStrArr:
                        if retrunStr == "DEPRECATED: Use of this script to execute hdfs command is deprecated.":
                            continue
                        elif retrunStr == "Instead use the hdfs command for it.":
                            continue
                        elif retrunStr == "":
                            continue
                        elif retrunStr.find("No such file or directory") >= 0:
                            continue
                        else:
                            def not_empty(s):
                                return s and s.strip()

                            pathSizeData = list(filter(not_empty, retrunStr.split(" ")))
                            pathData = list(filter(not_empty, pathSizeData[2].split("/")))
                            # mainData = ["資料時間", "Game名稱","DB名稱","Table名稱","容量大小"]
                            makeDataSizeDataInfo = copy.copy(makeInfo)
                            makeDataSizeDataInfo["databaseCtrl"] = None
                            makeDataSizeDataInfo["hdfsSSHCtrl"] = None
                            makeDataSizeDataInfo["pathData"] = pathData
                            makeDataSizeDataInfo["pathSizeData"] = pathSizeData
                            mainData = eval(f"self.makeDataSizeData_{makeName}({makeDataSizeDataInfo})")
                            mainDataList.append(mainData)
            except Exception as e:
                print(e)
                pass
        mainDF = pandas.DataFrame(mainDataList, columns=["datatime", "gamename", "dbname", "world", "tablename", "datasize"])
        mainDFgroup = mainDF.groupby(by=["datatime", "gamename", "dbname", "world", "tablename"], as_index=False)[["datasize"]].sum()
        return mainDFgroup

    def checkDataSize(self ,makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        gameNameArr = makeInfo["gameNameArr"] if "gameNameArr" in makeInfo.keys() else []
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None

        for gameName in gameNameArr:
            makeInfo["gameName"] = gameName
            sqlReplaceArr = [
                ['[:SelectDate]', makeTime.strftime("%Y-%m-%d")]
                , ['[:GameName]', gameName]
                , ['[:SchemaName]', schemaName]
                , ['[:MakeName]', makeName]
            ]
            deleteSqlStrs = """
                DELETE FROM [:SchemaName].datacheck
                WHERE 1 = 1
                    AND datatime >= '[:SelectDate]'::timestamp
                    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
                    AND ('Game' = '[:GameName]' OR gamename = '[:GameName]');

                DELETE FROM [:SchemaName].datacheckDetail
                WHERE 1 = 1
                    AND datatime >= '[:SelectDate]'::timestamp
                    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
                    AND ('Game' = '[:GameName]' OR gamename = '[:GameName]');
            """
            for sqlReplace in sqlReplaceArr:
                deleteSqlStrs = deleteSqlStrs.replace(sqlReplace[0], sqlReplace[1])
            deleteSqlStrArr = deleteSqlStrs.split(";")[:-1]
            for deleteSqlStr in deleteSqlStrArr:
                databaseCtrl.executeSQL(deleteSqlStr)

            # 目前都一樣未來可能會根據不同狀況有相關
            if makeName == "HadoopOriginal" :
                self.makeDataCheckDetail_HadoopOriginal(makeInfo)
            elif makeName == "HadoopExtract":
                self.makeDataCheckDetail_HadoopExtract(makeInfo)
            elif makeName == "ModelExtract":
                self.makeDataCheckDetail_ModelExtract(makeInfo)
            elif makeName == "BUReport":
                self.makeDataCheckDetail_BUReport(makeInfo)
            elif makeName == "TableauData":
                self.makeDataCheckDetail_TableauData(makeInfo)

            dataCheckSqlStrs = """
                INSERT INTO [:SchemaName].datacheck
                SELECT
                    AA.datatime
                    , AA.gamename
                    , AA.dbname
                    , SUM(AA.errorcount) as errorcount
                    , SUM(AA.errorsize) as errorsize
                    , null as message
                    , null as memo
                FROM [:SchemaName].datacheckdetail AA
                WHERE 1 = 1
                    AND datatime >= '[:SelectDate]'::timestamp
                    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
                    AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
                GROUP BY
                    AA.datatime
                    , AA.gamename
                    , AA.dbname ;
            """
            for sqlReplace in sqlReplaceArr:
                dataCheckSqlStrs = dataCheckSqlStrs.replace(sqlReplace[0], sqlReplace[1])
            dataCheckSqlStrArr = dataCheckSqlStrs.split(";")[:-1]
            for dataCheckSqlStr in dataCheckSqlStrArr:
                databaseCtrl.executeSQL(dataCheckSqlStr)

    def findDataCheckData (self, makeInfo):
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        gameNameArr = makeInfo["gameNameArr"] if "gameNameArr" in makeInfo.keys() else []
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None

        gameNames = "(''"
        for gameName in gameNameArr :
            gameNames = gameNames + ",'" + gameName + "'"
        gameNames = gameNames + ")"

        selectSqlSizeStr = """
            SELECT 
                CASE WHEN sum(datasize) IS NULL THEN 0 ELSE sum(datasize) END as datasize
            FROM [:SchemaName].datasize AAA
            WHERE 1 = 1 
                AND AAA.datatime >= '[:StartDate]' 
                AND AAA.datatime < '[:EndDate]'
                AND AAA.gamename in [:GameNames]
        """
        selectSqlSizeStr = selectSqlSizeStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
        selectSqlSizeStr = selectSqlSizeStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        selectSqlSizeStr = selectSqlSizeStr.replace("[:GameNames]", gameNames)
        selectSqlSizeStr = selectSqlSizeStr.replace("[:SchemaName]", schemaName)

        selectSqlTotalStr = """
            SELECT 
               AAA.datatime
               , AAA.gamename
               , SUM(errorcount) as errortablecount
               , SUM(errorsize) as errortablesize
            FROM [:SchemaName].datacheck AAA
            WHERE 1 = 1 
                AND AAA.datatime >= '[:StartDate]' 
                AND AAA.datatime < '[:EndDate]'
                AND AAA.gamename in [:GameNames]
            GROUP BY
               AAA.datatime
               , AAA.gamename
            ORDER BY
                AAA.datatime DESC
        """
        selectSqlTotalStr = selectSqlTotalStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
        selectSqlTotalStr = selectSqlTotalStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        selectSqlTotalStr = selectSqlTotalStr.replace("[:GameNames]", gameNames)
        selectSqlTotalStr = selectSqlTotalStr.replace("[:SchemaName]", schemaName)

        selectSqlDetailStr = """
            SELECT 
               AAA.*
            FROM [:SchemaName].datacheckdetail AAA
            WHERE 1 = 1 
                AND AAA.datatime >= '[:StartDate]' 
                AND AAA.datatime < '[:EndDate]'
                AND AAA.gamename in [:GameNames]
            ORDER BY
                AAA.datatime DESC
                , AAA.errorlevel
                , AAA.errorsize DESC
            limit 20
        """
        selectSqlDetailStr = selectSqlDetailStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
        selectSqlDetailStr = selectSqlDetailStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        selectSqlDetailStr = selectSqlDetailStr.replace("[:GameNames]", gameNames)
        selectSqlDetailStr = selectSqlDetailStr.replace("[:SchemaName]", schemaName)

        selectSqlTotal14Str = """
            SELECT  
               AAA.datatime
               , AAA.gamename
               , AAA.errorlevel
               , SUM(errorcount) as errortablecount
               , SUM(errorsize) as errortablesize
            FROM [:SchemaName].datacheckdetail AAA
            WHERE 1 = 1 
                AND AAA.datatime >= '[:StartDate]' 
                AND AAA.datatime < '[:EndDate]'
                AND AAA.gamename in [:GameNames]
            GROUP BY
               AAA.gamename
               , AAA.datatime
               , AAA.errorlevel
            ORDER BY
               AAA.gamename
               , AAA.datatime
               , AAA.errorlevel DESC
        """
        selectSqlTotal14Str = selectSqlTotal14Str.replace("[:StartDate]", (makeTime - datetime.timedelta(days=14)).strftime("%Y-%m-%d"))
        selectSqlTotal14Str = selectSqlTotal14Str.replace("[:EndDate]", (makeTime + datetime.timedelta(days=0)).strftime("%Y-%m-%d"))
        selectSqlTotal14Str = selectSqlTotal14Str.replace("[:GameNames]", gameNames)
        selectSqlTotal14Str = selectSqlTotal14Str.replace("[:SchemaName]", schemaName)

        checkSizeDF = databaseCtrl.searchSQL(selectSqlSizeStr)
        checkTotalDF = databaseCtrl.searchSQL(selectSqlTotalStr)
        checkDetailDF = databaseCtrl.searchSQL(selectSqlDetailStr)
        checkTotal14DF = databaseCtrl.searchSQL(selectSqlTotal14Str)
        return checkSizeDF , checkTotalDF , checkDetailDF , checkTotal14DF

    # ------------------------------------------------------------------------------------------

    def makeDataSizeData_HadoopOriginal(self, makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        pathData = makeInfo["pathData"] if "pathData" in makeInfo.keys() else {}
        pathSizeData = makeInfo["pathSizeData"] if "pathSizeData" in makeInfo.keys() else {}
        mainData = [None, None, None, None, None, None]

        if pathData[5] == "Log" or pathData[1] == "hdfs" :
            if gameName in ["lineage"] :
                mainData = [makeTime, pathData[2], "Log",pathData[6].replace("world=", ""), pathData[3] + "_" + pathData[4], int(pathSizeData[0])]
            else :
                if pathData[6] == "topics" :
                    mainData = [makeTime, pathData[3].replace(".db", ""), pathData[5], "COMMON", pathData[7], int(pathSizeData[0])]
                else :
                    mainData = [makeTime, pathData[3].replace(".db", ""), pathData[5], "COMMON", pathData[6], int(pathSizeData[0])]
        else:
            mainData = [makeTime, pathData[3].replace(".db", ""), pathData[5], "COMMON", pathData[7], int(pathSizeData[0])]

        return mainData

    def makeDataSizeData_HadoopExtract(self, makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        pathData = makeInfo["pathData"] if "pathData" in makeInfo.keys() else {}
        pathSizeData = makeInfo["pathSizeData"] if "pathSizeData" in makeInfo.keys() else {}
        mainData = [None, None, None, None, None, None]
        if gameName in ["maple"]:
            if pathData[4] == "middle" :
                if pathData[8].find("=") >= 0:
                    mainData = [makeTime, gameName , pathData[4], pathData[8].replace("gw=", "").replace("world=", ""), pathData[6], int(pathSizeData[0])]
                else:
                    mainData = [makeTime, gameName, pathData[4],  "COMMON", pathData[6], int(pathSizeData[0])]
            else:
                if pathData[6].find("world=") >= 0:
                    mainData = [makeTime, pathData[3].replace("_extract.db", ""), "extract", pathData[6].replace("world=", ""), pathData[4], int(pathSizeData[0])]
                else:
                    mainData = [makeTime, pathData[3].replace("_extract.db", ""), "extract", "COMMON", pathData[4], int(pathSizeData[0])]
        else :
            if pathData[7].find("world=") >= 0:
                mainData = [makeTime, pathData[3].replace("_extract.db", ""), pathData[4], pathData[7].replace("world=", ""), pathData[5], int(pathSizeData[0])]
            else:
                mainData = [makeTime, pathData[3].replace("_extract.db", ""), pathData[4], "COMMON", pathData[5], int(pathSizeData[0])]
        return mainData

    def makeDataSizeData_ModelExtract(self, makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        pathData = makeInfo["pathData"] if "pathData" in makeInfo.keys() else {}
        pathSizeData = makeInfo["pathSizeData"] if "pathSizeData" in makeInfo.keys() else {}
        mainData = [None, None, None, None, None, None]
        mainData = [makeTime, pathData[5].replace("game=", ""), "modelextract", pathData[7].replace("world=", ""), pathData[8].replace("tablenumber=", ""), int(pathSizeData[0])]
        return mainData

    def makeDataSizeData_BUReport(self,makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        pathData = makeInfo["pathData"] if "pathData" in makeInfo.keys() else {}
        pathSizeData = makeInfo["pathSizeData"] if "pathSizeData" in makeInfo.keys() else {}
        mainData = [None, None, None, None, None, None]
        mainData = [makeTime, pathData[5].replace("game=", ""), "bureport", pathData[7].replace("world=", ""), pathData[8].replace("tablenumber=", ""), int(pathSizeData[0])]
        return mainData

    def makeDataSizeData_TableauData(self,makeInfo):
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        rowData = makeInfo["rowData"] if "rowData" in makeInfo.keys() else {}
        mainData = [None, None, None, None, None, None]
        mainData = [makeTime, rowData[0], "tableaudata", rowData[1] , rowData[2], rowData[3]]
        return mainData
    # ------------------------------------------------------------------------------------------
    def getHadoopSHStrArr_HadoopOriginal(self,makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        if gameName in ["lineage"]:
            return [
                "hdfs dfs -du -s /user/hive/warehouse/lineage.db/ALL/*/dt=[:DateNoLine]/*"
                , "hdfs dfs -du -s /user/hdfs/lineage/*/*/date=[:DateNoLine]/world=*"
            ]
        else:
            return [
                "hdfs dfs -du -s /user/hive/warehouse/[:GameName].db/ALL/*/dt=[:DateNoLine]/*"
                , "hdfs dfs -du -s /user/hive/warehouse/[:GameName].db/ALL/Log/*/dt=[:YYMMNoLine]01"
                , "hdfs dfs -du -s /user/hive/warehouse/[:GameName].db/ALL/Log/topics/*/dt=[:DateNoLine]"
            ]

    def getHadoopSHStrArr_HadoopExtract(self,makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        if gameName in ["maple"]:
            return [
                "hdfs dfs -du -s /user/hive/warehouse/maple_extract.db/[!a]*/dt=[:DateNoLine]/[!w]*/"
                , "hdfs dfs -du -s /user/hive/warehouse/maple_extract.db/[!a]*/dt=[:DateNoLine]/world=*/*"
                , "hdfs dfs -du -s /user/GTW_PD/DB/Maple/middle/Process/*/dt=[:DateNoLine]/[!gw]*"
                , "hdfs dfs -du -s /user/GTW_PD/DB/Maple/middle/Process/*/dt=[:DateNoLine]/[gw]*/*"
            ]
        else :
            return ["hdfs dfs -du -s /user/hive/warehouse/[:GameName]_extract.db/[!a]*/*/dt=[:DateNoLine]/[!w]*/"
                , "hdfs dfs -du -s /user/hive/warehouse/[:GameName]_extract.db/[!a]*/*/dt=[:DateNoLine]/world=*/*"]

    def getHadoopSHStrArr_ModelExtract(self,makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        return ["hdfs dfs -du -s /user/GTW_PD/DB/ModelExtract/modelextract/game=[:GameName]/dt=[:DateNoLine]/world=*/tablenumber=*"]

    def getHadoopSHStrArr_BUReport(self,makeInfo):
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else []
        return ["hdfs dfs -du -s /user/GTW_PD/DB/Business/bureport/game=[:GameName]/dt=[:DateNoLine]/world=*/tablenumber=*"]

    # ------------------------------------------------------------------------------------------

    def makeDataCheckDetail_HadoopOriginal(self, makeInfo):
        self.checkNewAddTable_Original(makeInfo)
        self.makeDataCheckDetail_Common(makeInfo)

    def makeDataCheckDetail_HadoopExtract(self, makeInfo):
        self.checkNewAddTable_Common(makeInfo)
        self.makeDataCheckDetail_Common(makeInfo)

    def makeDataCheckDetail_ModelExtract(self, makeInfo):
        self.makeDataCheckDetail_Common(makeInfo)

    def makeDataCheckDetail_BUReport(self, makeInfo):
        self.makeDataCheckDetail_Common(makeInfo)

    def makeDataCheckDetail_TableauData(self, makeInfo):
        self.makeDataCheckDetail_Common(makeInfo)

    # ------------------------------------------------------------------------------------------
    def checkNewAddTable_Original(self, makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None
        sqlReplaceArr = [
            ['[:SelectDate]', makeTime.strftime("%Y-%m-%d")]
            , ['[:GameName]', gameName]
            , ['[:SchemaName]', schemaName]
            , ['[:MakeName]', makeName]
        ]
        checkNewAddTableSqlStrs = """
            INSERT INTO [:SchemaName].newaddtable
            SELECT AAA.logdate ,AAA.gamename ,AAA.dbname ,AAA.tablename
            FROM (
                SELECT '[:SelectDate]'::timestamp AS logdate ,AA.* ,coalesce(BB.errorlevel,'D') AS errorlevel
                FROM (
                    SELECT DISTINCT gamename ,dbname ,tablename 
                    FROM [:SchemaName].datasize 
                    WHERE gamename = '[:GameName]'
                    AND   datasize > 0
                    AND   datatime = '[:SelectDate]'
                ) AA
                LEFT OUTER JOIN (
                    SELECT DISTINCT gamename ,dbname ,tablename ,errorlevel 
                    FROM hadoopextract.datacheckin
                ) BB
                ON AA.gamename = BB.gamename AND lower(AA.tablename) = lower(BB.tablename) AND lower(AA.dbname) = lower(BB.dbname)
                WHERE lower(AA.tablename) NOT LIKE '%_20%'	
                AND   lower(AA.tablename) NOT LIKE '%_tmp%'
                AND   lower(AA.tablename) NOT LIKE '%tmp_%'
                AND   lower(AA.tablename) NOT LIKE '%_temp%'
                AND   lower(AA.tablename) NOT LIKE '%temp_%'
                AND   lower(AA.tablename) NOT LIKE '%_bak%'
                AND   lower(AA.tablename) NOT LIKE '%bak_%'
                AND   lower(AA.tablename) NOT LIKE '%backup%'
                AND   lower(AA.tablename) NOT LIKE '%test%'
            ) AAA
            LEFT OUTER JOIN [:SchemaName].datacheckin BBB
            ON AAA.gamename = BBB.gamename AND AAA.tablename = BBB.tablename 
            WHERE BBB.tablename IS NULL
            ;
        """
        # 將相關內容字串取代成相關字元
        for sqlReplace in sqlReplaceArr:
            checkNewAddTableSqlStrs = checkNewAddTableSqlStrs.replace(sqlReplace[0], sqlReplace[1])
        checkNewAddTableSqlStrArr = checkNewAddTableSqlStrs.split(";")[:-1]
        for checkNewAddTableSqlStr in checkNewAddTableSqlStrArr:
            databaseCtrl.executeSQL(checkNewAddTableSqlStr)

        addDataCheckInSqlStrs = """
            INSERT INTO [:SchemaName].datacheckin
            SELECT AAA.gamename ,AAA.dbname ,AAA.tablename ,AAA.errorlevel
            FROM (
                SELECT '[:SelectDate]'::timestamp AS logdate ,AA.* ,coalesce(BB.errorlevel,'B') AS errorlevel
                FROM (
                    SELECT DISTINCT gamename ,dbname ,tablename 
                    FROM [:SchemaName].datasize 
                    WHERE gamename = '[:GameName]'
                    AND   datasize > 0
                    AND   datatime = '[:SelectDate]'
                ) AA
                LEFT OUTER JOIN (
                    SELECT DISTINCT gamename ,dbname ,tablename ,errorlevel 
                    FROM hadoopextract.datacheckin
                    WHERE gamename = '[:GameName]'
                ) BB
                ON AA.gamename = BB.gamename AND lower(AA.tablename) = lower(BB.tablename) AND lower(AA.dbname) = lower(BB.dbname)
                WHERE lower(AA.tablename) NOT LIKE '%_20%'	
                AND   lower(AA.tablename) NOT LIKE '%_tmp%'
                AND   lower(AA.tablename) NOT LIKE '%tmp_%'
                AND   lower(AA.tablename) NOT LIKE '%_temp%'
                AND   lower(AA.tablename) NOT LIKE '%temp_%'
                AND   lower(AA.tablename) NOT LIKE '%_bak%'
                AND   lower(AA.tablename) NOT LIKE '%bak_%'
                AND   lower(AA.tablename) NOT LIKE '%backup%'
                AND   lower(AA.tablename) NOT LIKE '%test%'
            ) AAA
            LEFT OUTER JOIN [:SchemaName].datacheckin BBB
            ON AAA.gamename = BBB.gamename AND AAA.tablename = BBB.tablename 
            WHERE BBB.tablename IS NULL
            ;
        """
        # 將相關內容字串取代成相關字元
        for sqlReplace in sqlReplaceArr:
            addDataCheckInSqlStrs = addDataCheckInSqlStrs.replace(sqlReplace[0], sqlReplace[1])
        addDataCheckInSqlStrArr = addDataCheckInSqlStrs.split(";")[:-1]
        for addDataCheckInSqlStr in addDataCheckInSqlStrArr:
            databaseCtrl.executeSQL(addDataCheckInSqlStr)

    def checkNewAddTable_Common(self, makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None
        sqlReplaceArr = [
            ['[:SelectDate]', makeTime.strftime("%Y-%m-%d")]
            , ['[:GameName]', gameName]
            , ['[:SchemaName]', schemaName]
            , ['[:MakeName]', makeName]
        ]
        checkNewAddTableSqlStrs = """
            INSERT INTO [:SchemaName].newaddtable
            SELECT 
                '[:SelectDate]'::timestamp AS logdate,
                AA.*
            FROM (
                SELECT DISTINCT gamename ,dbname ,tablename 
                FROM [:SchemaName].datasize 
                WHERE gamename = '[:GameName]'
                AND   datasize > 0
                AND   datatime = '[:SelectDate]'
            ) AA
            LEFT OUTER JOIN (
                SELECT DISTINCT gamename ,dbname ,tablename 
                FROM [:SchemaName].datacheckin
                WHERE gamename = '[:GameName]'
            ) BB
            ON AA.gamename = BB.gamename AND LOWER(AA.tablename) = LOWER(BB.tablename)
            WHERE BB.tablename IS NULL ;
        """
        # 將相關內容字串取代成相關字元
        for sqlReplace in sqlReplaceArr:
            checkNewAddTableSqlStrs = checkNewAddTableSqlStrs.replace(sqlReplace[0], sqlReplace[1])
        checkNewAddTableSqlStrArr = checkNewAddTableSqlStrs.split(";")[:-1]
        for checkNewAddTableSqlStr in checkNewAddTableSqlStrArr:
            databaseCtrl.executeSQL(checkNewAddTableSqlStr)

        addDataCheckInSqlStrs = """
            INSERT INTO [:SchemaName].datacheckin
            SELECT 
                AA.*,
                'B' AS errorlevel
            FROM (
                SELECT DISTINCT gamename ,dbname ,tablename 
                FROM [:SchemaName].datasize 
                WHERE gamename = '[:GameName]'
                AND   datasize > 0
                AND   datatime = '[:SelectDate]'
            ) AA
            LEFT OUTER JOIN (
                SELECT DISTINCT gamename ,dbname ,tablename 
                FROM [:SchemaName].datacheckin
                WHERE gamename = '[:GameName]'
            ) BB
            ON AA.gamename = BB.gamename and LOWER(AA.tablename) = LOWER(BB.tablename)
            WHERE BB.tablename is null ;
        """
        # 將相關內容字串取代成相關字元
        for sqlReplace in sqlReplaceArr:
            addDataCheckInSqlStrs = addDataCheckInSqlStrs.replace(sqlReplace[0], sqlReplace[1])
        addDataCheckInSqlStrArr = addDataCheckInSqlStrs.split(";")[:-1]
        for addDataCheckInSqlStr in addDataCheckInSqlStrArr:
            databaseCtrl.executeSQL(addDataCheckInSqlStr)

    def makeDataCheckDetail_Common(self, makeInfo):
        makeName = makeInfo["makeName"] if "makeName" in makeInfo.keys() else ""
        gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else ""
        makeTime = makeInfo["makeTime"] if "makeTime" in makeInfo.keys() else datetime.datetime.now()
        schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else ""
        databaseCtrl = makeInfo["databaseCtrl"] if "databaseCtrl" in makeInfo.keys() else None
        sqlReplaceArr = [
            ['[:SelectDate]', makeTime.strftime("%Y-%m-%d")]
            , ['[:GameName]', gameName]
            , ['[:SchemaName]', schemaName]
            , ['[:MakeName]', makeName]
        ]
        dataCheckDetailSqlStrs = """
           INSERT INTO [:SchemaName].datacheckdetail
           WITH BASIC_DATA as (
               SELECT
                   AA.gamename
                   , AA.dbname
                   , AA.world
                   , AA.tablename
                   , SUM(CASE WHEN datatime = '[:SelectDate]' THEN datasize ELSE 0 END) as datanowday
                   , Round(
                       SUM(
                           CASE
                               WHEN 1 = 1
                               AND datatime <= '[:SelectDate]'::timestamp - INTERVAL '1 Day'
                               AND datatime >= '[:SelectDate]'::timestamp - INTERVAL '6 Day'
                           THEN datasize
                           ELSE 0
                           END
                       )/6
                   ) as data7bmean
                   , 0 as dbnowday
                   , 0 as db7bmean
               FROM [:SchemaName].datasize AA
               WHERE 1 = 1
                   AND AA.datatime >= '[:SelectDate]'::timestamp - INTERVAL '6 Day'
                   AND AA.datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
                   AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
               group by
                   AA.gamename
                   , AA.dbname
                   , AA.world
                   , AA.tablename
           )
           SELECT
               '[:SelectDate]'::timestamp as datatime
               , AAAA.gamename
               , AAAA.dbname
               , AAAA.world
               , AAAA.tablename
               , CASE 
                   WHEN '[:MakeName]' in ('ModelExtract','BUReport','TableauData') AND CCCC.errorlevel is not null THEN CCCC.errorlevel 
                   WHEN '[:MakeName]' in ('ModelExtract','BUReport','TableauData') THEN 'A' 
                   WHEN CCCC.errorlevel is null THEN 'D' 
                   ELSE CCCC.errorlevel 
                 END as errorlevel
               , AAAA.datanowday
               , AAAA.data7bmean
               , 1 as errorcount
               , AAAA.data7bmean - AAAA.datanowday as errorsize
               , 'Today Data size does not meet the requirements.(Lower than the seven-day average)' as message
               , '{nowday:'||AAAA.datanowday||',bmean:'||AAAA.data7bmean||',datapar:'||AAAA.datapar||'}' as memo
           FROM (
               SELECT
                   AAA.gamename
                   , AAA.dbname
                   , AAA.world
                   , AAA.tablename
                   , SUM(AAA.datanowday) as datanowday
                   , SUM(AAA.data7bmean) as data7bmean
                   , case when SUM(AAA.data7bmean) = 0 then 0 else SUM(AAA.datanowday) / SUM(AAA.data7bmean) end as datapar
                   , SUM(AAA.dbnowday) as dbnowday
                   , SUM(AAA.db7bmean) as db7bmean
                   , case when SUM(AAA.db7bmean) = 0 then 0 else SUM(AAA.dbnowday) / SUM(AAA.db7bmean) end  as dbpar
               FROM BASIC_DATA AAA
               WHERE 1 = 1
                   AND AAA.data7bmean != 0 
               GROUP BY
                   AAA.gamename
                   , AAA.dbname
                   , AAA.tablename
                   , AAA.world
           ) AAAA
           LEFT join [:SchemaName].datacheckin CCCC on 1 = 1
               AND AAAA.gamename = CCCC.gamename
               AND (AAAA.dbname = CCCC.dbname or CCCC.dbname is null)
               AND (AAAA.tablename = CCCC.tablename or CCCC.tablename is null)
           LEFT join [:SchemaName].datachecknoin DDDD on 1 = 1
               AND AAAA.gamename = DDDD.gamename
               AND (AAAA.dbname = DDDD.dbname or DDDD.dbname is null)
               AND (AAAA.tablename = DDDD.tablename or DDDD.tablename is null)
           WHERE 1 = 1
               AND AAAA.datapar < 0.65
               AND AAAA.datanowday = 0
               AND AAAA.data7bmean != 0 
               AND DDDD.tablename is null;
        """
        # 將相關內容字串取代成相關字元
        for sqlReplace in sqlReplaceArr:
            dataCheckDetailSqlStrs = dataCheckDetailSqlStrs.replace(sqlReplace[0], sqlReplace[1])
        dataCheckDetailSqlStrArr = dataCheckDetailSqlStrs.split(";")[:-1]
        for dataCheckDetailSqlStr in dataCheckDetailSqlStrArr:
            databaseCtrl.executeSQL(dataCheckDetailSqlStr)

    def sizeToStr(self,filesize):
        if filesize >= 1000000000000:
            return str(round(filesize / (1024*1024*1024*1024), 3)) + "T"
        elif filesize >= 1000000000:
            return str(round(filesize / (1024*1024*1024), 2)) + "G"
        elif filesize >= 1000000:
            return str(round(filesize / (1024*1024), 2)) + "M"
        elif filesize >= 1000:
            return str(round(filesize / (1024), 2)) + "K"
        else:
            return str(round(filesize, 2)) + "B"
