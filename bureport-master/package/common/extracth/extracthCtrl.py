import pandas
import time
import asyncio

class ExtracthCtrl:

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._startTime = time.time()

    def MakeWorldDataDetail (self,makeTime,hiveCtrl,databaseInfo) :

        gameName = databaseInfo["gameName"]
        dbName = databaseInfo["dbName"]
        worldNameArr = databaseInfo["worldNameArr"]
        partitionedAlterInit = databaseInfo["partitionedAlterInit"]
        partitionedPathInit = databaseInfo["partitionedPathInit"]
        partitionedSQLParentInit = databaseInfo["partitionedSQLInit"]
        partitionedSQLInit = databaseInfo["partitionedSQLInit"]
        typeArray = databaseInfo["typeArray"] if "typeArray" in databaseInfo.keys() else []
        tableNameArray = databaseInfo["tableNameArray"] if "tableNameArray" in databaseInfo.keys() else []

        tableDataFrame = pandas.read_csv(databaseInfo["csvfile"], encoding="utf_8_sig")

        #利用排序決定執行順序
        tableDataFrame = tableDataFrame.sort_values(by=["layers", "type"])
        print("Start Insert", dbName, makeTime.strftime("%Y-%m-%d"), "Data.")
        exLayer = 0
        while exLayer <= 100:
            # print(exLayer)
            taskSQLStrArr = []
            # print("{} exLayer : {}".format(makeTime.strftime("%Y-%m-%d"), str(exLayer)))
            exLayerTableDataFrame = tableDataFrame[tableDataFrame["layers"].isin([exLayer])]

            if len(exLayerTableDataFrame) == 0:
                exLayer = exLayer + 1
                continue

            for tableIndex, tableRow in exLayerTableDataFrame.iterrows():
                if typeArray != [] and tableRow["type"] not in typeArray:
                    continue

                if tableNameArray != [] and tableRow["tablename"] not in tableNameArray:
                    continue

                if tableRow["type"] == "grammar":
                    insertDataInitSQL = self.getInsertGrammarDataSQL()
                elif tableRow["type"] == "log":
                    insertDataInitSQL = self.getInsertLogDataSQL()
                elif tableRow["type"] == "logfile":
                    insertDataInitSQL = self.getInsertAllDataSQL()
                else:
                    insertDataInitSQL = self.getInsertAllDataSQL()

                #if tableRow["oritablename"] != "characters" :
                #    continue

                oriColumnsAllStr = ""
                oriShowColumnsSQL = "show columns in {}_{}.{}_{}".format(str(tableRow["gamename"]), str(tableRow["orihivedb"]), str(tableRow["oridbname"]), str(tableRow["oritablename"]))
                oriColumnsDataFrame = hiveCtrl.searchSQL_TCByCount(oriShowColumnsSQL,3)
                oriColumnsDataFrame = oriColumnsDataFrame[~oriColumnsDataFrame["field"].isin(["dt", "world"])]
                for columnsIndex, columnsRow in oriColumnsDataFrame.iterrows():
                    oriColumnsAllStr = ("ORI." + columnsRow["field"]) if oriColumnsAllStr == "" else (oriColumnsAllStr + ",ORI." + columnsRow["field"])

                showColumnsSQL = "show columns in {}_{}.{}_{}".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"]))

                try:
                    try:
                        hiveCtrl.executeSQL(showColumnsSQL)
                    except:
                        createStrInit = "CREATE External TABLE IF NOT EXISTS [:tableFullName] like [:OriTableFullName] Location '[:FilePath]' \n"
                        OriTableFullName = "{}_{}.{}_{}".format(tableRow["gamename"], tableRow["orihivedb"],tableRow["oridbname"], tableRow["oritablename"])
                        tableFullName = "{}_{}.{}_{}".format(tableRow["gamename"], tableRow["hivedb"], tableRow["dbname"],tableRow["tablename"])
                        createStr = createStrInit.replace("[:OriTableFullName]", OriTableFullName).replace("[:tableFullName]",tableFullName)
                        createStr = createStr.replace("[:FilePath]", databaseInfo["filePathInit"])
                        print(createStr)
                        hiveCtrl.executeSQL(createStr)
                except:
                    pass

                for worldName in worldNameArr:

                    partitionedLogSQLInit = ''
                    if tableRow["type"] == "log":
                        if worldName == '':
                            partitionedLogSQLInit = " ORI.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',[:DTBeforeDay]),'yyyyMMdd') " \
                                                    " AND ORI.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',[:DTAfterDay]),'yyyyMMdd') "
                        else:
                            partitionedLogSQLInit = " ORI.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',[:DTBeforeDay]),'yyyyMMdd') " \
                                                    " AND ORI.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',[:DTAfterDay]),'yyyyMMdd') " \
                                                    " AND world = '[:World]'"

                    # 2020/06/22 修正 目前grammar要判斷相關parent表的partition是否存在dt與world，並且連帶判斷ori表是否有相對應的dt與world
                    if tableRow["type"] == "grammar":
                        parentShowColumnsSQL = "show columns in {}_{}.{}_{}".format(str(tableRow["gamename"]),str(tableRow["parenthivedb"]),str(tableRow["parentdbname"]),str(tableRow["parenttablename"]))
                        parentColumnsDataFrame = hiveCtrl.searchSQL_TCByCount(parentShowColumnsSQL,3)
                        if "dt" in parentColumnsDataFrame["field"].values and "dt" in partitionedSQLInit :
                            partitionedSQLParentInit = "dt='[:DateNoLine]' "

                        if "world" in parentColumnsDataFrame["field"].values and "world" in partitionedSQLInit :
                            partitionedSQLParentInit += "AND world = '[:World]'"

                    partitionedSQLParentInit = partitionedSQLParentInit.replace("dt='[:DateNoLine]'", "dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')")
                    partitionedSQLOriInit = partitionedSQLInit.replace("dt='[:DateNoLine]'", "dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')")

                    insertDataSQL = insertDataInitSQL.replace("[:PartitionedPath]", partitionedPathInit)
                    insertDataSQL = insertDataSQL.replace("[:PartitionedAlter]", partitionedAlterInit)
                    insertDataSQL = insertDataSQL.replace("[:ParentPartitionedSQL]", partitionedSQLParentInit)
                    insertDataSQL = insertDataSQL.replace("[:OriPartitionedSQL]", partitionedSQLOriInit)
                    insertDataSQL = insertDataSQL.replace("[:partitionedLogSQL]", partitionedLogSQLInit)

                    insertDataSQL = insertDataSQL.replace("[:GameName]", str(tableRow["gamename"]))
                    insertDataSQL = insertDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
                    insertDataSQL = insertDataSQL.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d"))
                    insertDataSQL = insertDataSQL.replace("[:World]", worldName)
                    insertDataSQL = insertDataSQL.replace("[:OriColumnsAll]", oriColumnsAllStr)
                    insertDataSQL = insertDataSQL.replace("[:ParentHiveDB]", str(tableRow["parenthivedb"])).replace("[:ParentDBName]",str(tableRow["parentdbname"]))
                    insertDataSQL = insertDataSQL.replace("[:ParentTableName]",str(tableRow["parenttablename"])).replace("[:ParentColumnName]", str(tableRow["parentcolumnname"]))
                    insertDataSQL = insertDataSQL.replace("[:OriHiveDB]", str(tableRow["orihivedb"])).replace("[:OriDBName]",str(tableRow["oridbname"]))
                    insertDataSQL = insertDataSQL.replace("[:OriTableName]",str(tableRow["oritablename"])).replace("[:OriColumnName]", str(tableRow["oricolumnname"]))
                    insertDataSQL = insertDataSQL.replace("[:HiveDB]", str(tableRow["hivedb"])).replace("[:DBName]",str(tableRow["dbname"]))
                    insertDataSQL = insertDataSQL.replace("[:TableName]",str(tableRow["tablename"])).replace("[:ColumnName]", str(tableRow["columnname"]))
                    insertDataSQL = insertDataSQL.replace("[:DTBeforeDay]", str(tableRow["dtbeforeday"]).split('.')[0]).replace("[:DTAfterDay]", str(tableRow["dtafterday"]).split('.')[0])
                    insertDataSQL = insertDataSQL.replace("[:LogStartDateName]", str(tableRow["logstartdate"])).replace("[:LogEndDateName]",str(tableRow["logenddate"]))
                    # print(insertDataSQL)
                    taskSQLStrArr.append(insertDataSQL)

            self.runsql(hiveCtrl, taskSQLStrArr)
            print("Insert", makeTime.strftime("%Y-%m-%d"), "Data Total Used", time.time() - self._startTime, "seconds.")
            exLayer = exLayer + 1

    def getInsertGrammarDataSQL(self):
        return """
            -- [:GameName] MakeData
            WITH BasicData AS (
                SELECT 
                    AA.[:ParentColumnName]
                FROM [:GameName]_[:ParentHiveDB].[:ParentDBName]_[:ParentTableName] AA
                WHERE 1 = 1 
                    AND [:ParentPartitionedSQL]
                GROUP BY
                    AA.[:ParentColumnName]
            )
            INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]'
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            SELECT [:OriColumnsAll]
            FROM BasicData AAA
            INNER JOIN [:GameName]_[:OriHiveDB].[:OriDBName]_[:OriTableName] ORI ON 1 = 1
                AND AAA.[:ParentColumnName] = ORI.[:OriColumnName]
                AND [:OriPartitionedSQL] 
            WHERE 1 = 1 ; 

            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] DROP IF EXISTS PARTITION ( [:PartitionedAlter] ) ;
            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] ADD IF NOT EXISTS PARTITION ( [:PartitionedAlter] ) 
                    LOCATION '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]';
        """

    def getInsertLogDataSQL(self):
        return """
            -- [:GameName] MakeData
            INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]'
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            SELECT DISTINCT [:OriColumnsAll]
            FROM [:GameName]_[:OriHiveDB].[:OriDBName]_[:OriTableName] ORI
            WHERE 1 = 1
                AND ORI.[:LogStartDateName] < DATE_ADD('[:DateLine]',1)
                AND ORI.[:LogEndDateName] >= DATE_ADD('[:DateLine]',0)
                AND [:partitionedLogSQL]; 

            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] DROP IF EXISTS PARTITION ( [:PartitionedAlter] ) ;
            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] ADD IF NOT EXISTS PARTITION ( [:PartitionedAlter] ) 
                    LOCATION '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]';
        """

    def getInsertLogFile(self):
        return """
            -- [:GameName] MakeData
            INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]'
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            SELECT [:OriColumnsAll]
            FROM [:GameName]_[:OriHiveDB].[:OriDBName]_[:OriTableName] ORI
            WHERE 1 = 1
                AND ORI.[:LogStartDateName] < DATE_ADD('[:DateLine]',1)
                AND ORI.[:LogEndDateName] >= DATE_ADD('[:DateLine]',0)
                AND ORI.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd') ; 

            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] DROP IF EXISTS PARTITION ( [:PartitionedAlter] ) ;
            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] ADD IF NOT EXISTS PARTITION ( [:PartitionedAlter] ) 
                    LOCATION '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]';
        """

    def getInsertAllDataSQL(self):
        return """
            -- [:GameName] MakeData
            INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]'
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            SELECT [:OriColumnsAll]
            FROM [:GameName]_[:OriHiveDB].[:OriDBName]_[:OriTableName] ORI
            WHERE 1 = 1
                AND [:OriPartitionedSQL]  ; 

            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] DROP IF EXISTS PARTITION ( [:PartitionedAlter] ) ;
            ALTER TABLE [:GameName]_[:HiveDB].[:DBName]_[:TableName] ADD IF NOT EXISTS PARTITION ( [:PartitionedAlter] ) 
                    LOCATION '/user/hive/warehouse/[:GameName]_[:HiveDB].db/[:DBName]/[:TableName]/[:PartitionedPath]';
        """

    # 若SQL中有分號，需先取代為[:SEMICOLON]
    def runsql(self, hiveCtrl, taskSQLStrArr,printError=False):
        if taskSQLStrArr != []:
            tasks = []
            semaphore = asyncio.Semaphore(40)
            count = 0
            for sqlStr in taskSQLStrArr:
                count = count + 1
                task = asyncio.ensure_future(self.send_req(hiveCtrl, sqlStr, semaphore, count , printError))
                tasks.append(task)
            self._loop.run_until_complete(asyncio.wait(tasks))

    async def send_req(self, hiveCtrl, insertDataSQL, semaphore, count,printError):
        async with semaphore:
            endTime = time.time()
            if printError == True:
                print("Send a request at", endTime - self._startTime, "seconds. {}".format(str(count)))
            try:
                res = await self._loop.run_in_executor(None, self.executeSQL, hiveCtrl, insertDataSQL,printError)
            except:
                print("error {}".format(str(count)))
                if printError == True:
                    print(insertDataSQL)
            endTime = time.time()
            if printError == True :
                print("Receive a response at", endTime - self._startTime, "seconds. {}".format(str(count)))

    def executeSQL(self,hiveCtrl,sqlStrs,printError):
        sqlStrArr = sqlStrs.split(";")[:-1]
        for sqlStr in sqlStrArr:
            # 若SQL中有分號，需先取代為[:SEMICOLON]
            sqlStr = sqlStr.replace("[:SEMICOLON]",";")
            hiveCtrl.executeSQL_TCByCount(sqlStr, 3,printError)

    def makeExecuteCSV (self,gamename,dbnname,startdate,enddate,tableDF,tableCheck,noFilterNameArray):
        objectArr = []
        for index, row in tableDF.iterrows():
            tableFullName = row["TABLE_NAME"]
            tableFullLowerName = row["TABLE_NAME"].lower()

            if row["TABLE_TYPE"] != "BASE TABLE":
                continue

            isMake = True
            for noFilterName in noFilterNameArray:
                if noFilterName in tableFullLowerName:
                    isMake = False

            if isMake == False:
                continue

            for numberStr in ["","@1","@2","@3","@4","@5","@6","@7","@8","@9"] :
                tableMemoOri = tableCheck[tableFullName+numberStr] if tableFullName+numberStr in tableCheck else None
                if tableMemoOri != None :
                    tableMemoArr = tableMemoOri.split(" ")
                    if tableMemoArr[1] == "EXCLUDE":
                        continue
                    object = self.makeExecuteObject(tableMemoArr, gamename, dbnname, tableFullLowerName, startdate, enddate)
                    objectArr.append(object)

        objectDF = pandas.core.frame.DataFrame(objectArr)

        objectDF = objectDF.ix[:, ["type", "layers", "gamename", "startdate", "enddate"
                                      , "parenthivedb", "parentdbname", "parenttablename", "parentcolumnname"
                                      , "orihivedb", "oridbname", "oritablename", "oricolumnname"
                                      , "hivedb", "dbname", "tablename", "columnname"
                                      , "logstartdate", "logenddate", "dtbeforeday", "dtafterday", "logfilename", "log",
                                   "sql", "memo", "memo2"]]

        objectDF = objectDF.sort_values(by=["type", "layers", "oritablename"])

        return objectDF

    def makeExecuteObject(self,tableMemoArr,gamename,dbnname,tablename,startdate,enddate):
        object = {}
        print(tableMemoArr)
        if tableMemoArr[1] == "ALL":
            object["type"] = "all"
        elif tableMemoArr[1] == "LOG":
            object["type"] = "log"
        else:
            object["type"] = "grammar"

        object["layers"] = int(tableMemoArr[5])

        object["gamename"] = gamename
        object["startdate"] = startdate
        object["enddate"] = enddate

        if tableMemoArr[1] == "ALL" or tableMemoArr[1] == "LOG":
            object["parenthivedb"] = None
            object["parentdbname"] = None
            object["parenttablename"] = None
            object["parentcolumnname"] = None
        else:
            object["parenthivedb"] = tableMemoArr[6].split(".")[0].split("_")[1].lower()
            object["parentdbname"] = tableMemoArr[6].split(".")[1].split("_")[0].lower()
            object["parenttablename"] = tableMemoArr[6].split(".")[1].replace(
                tableMemoArr[6].split(".")[1].split("_")[0] + "_", "").lower()
            object["parentcolumnname"] = tableMemoArr[1].lower()

        if len(tableMemoArr) <= 6 or tableMemoArr[7] == "None":
            object["orihivedb"] = "all"
            object["oridbname"] = dbnname
            object["oritablename"] = tablename
            if tableMemoArr[1] == "ALL" or tableMemoArr[1] == "LOG":
                object["oricolumnname"] = None
            else:
                object["oricolumnname"] = tableMemoArr[2].lower()
        elif tableMemoArr[7].split('.')[1].split(dbnname+'_')[0] == '':
            object["orihivedb"] = tableMemoArr[7].split(".")[0].split("_")[1].lower()
            object["oridbname"] = dbnname
            object["oritablename"] = tableMemoArr[7].split('.')[1].split(dbnname+'_')[1].lower()
            object["oricolumnname"] = tableMemoArr[2].lower()
        else:
            object["orihivedb"] = tableMemoArr[7].split(".")[0].split("_")[1].lower()
            object["oridbname"] = tableMemoArr[7].split(".")[1].split("_")[0].lower()
            object["oritablename"] = tableMemoArr[7].split(".")[1].replace(
                tableMemoArr[7].split(".")[1].split("_")[0] + "_", "").lower()
            object["oricolumnname"] = tableMemoArr[2].lower()

        if len(tableMemoArr) <= 6 or tableMemoArr[7] == "None":
            object["hivedb"] = "extract"
            object["dbname"] = dbnname
            object["tablename"] = tablename
            object["columnname"] = None
        elif tableMemoArr[8].split('.')[1].split(dbnname + '_')[0] == '':
            object["hivedb"] = tableMemoArr[8].split(".")[0].split("_")[1].lower()
            object["dbname"] = dbnname
            object["tablename"] = tableMemoArr[8].split('.')[1].split(dbnname+'_')[1].lower()
            object["columnname"] = None
        else:
            object["hivedb"] = tableMemoArr[8].split(".")[0].split("_")[1].lower()
            object["dbname"] = tableMemoArr[8].split(".")[1].split("_")[0].lower()
            object["tablename"] = tableMemoArr[8].split(".")[1].replace(
                tableMemoArr[8].split(".")[1].split("_")[0] + "_", "").lower()
            object["columnname"] = None

        if tableMemoArr[1] == "LOG":
            object["logstartdate"] = tableMemoArr[3].lower()
            object["logenddate"] = tableMemoArr[4].lower()
            object["dtbeforeday"] = "0"
            object["dtafterday"] = "1"
        else:
            object["logstartdate"] = None
            object["logenddate"] = None
            object["dtbeforeday"] = None
            object["dtafterday"] = None

        object["logfilename"] = None
        object["log"] = None
        object["sql"] = None
        object["memo"] = tableMemoArr[0]
        object["memo2"] = None

        return object