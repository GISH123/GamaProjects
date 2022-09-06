import os
from package.common.database.sqlTool import SqlTool
import package.common.common.GamaniaDateTime as gamaniaDateTime
import datetime
import copy

sqlTool = SqlTool()

class BUReportCtrl:

    def makeOtherDetil(self, makeInfo):
        gameName = makeInfo["gameName"]
        reportName = makeInfo["reportName"]
        tableNumberArr = makeInfo["tableNumberArr"] if "tableNumberArr" in makeInfo.keys() else []
        layerArr = makeInfo["layerArr"] if "layerArr" in makeInfo.keys() else []
        layerInfoArrMap = makeInfo["layerInfoArrMap"] if "layerInfoArrMap" in makeInfo.keys() else []
        tableNumberInfoMap = makeInfo["tableNumberInfoMap"] if "tableNumberInfoMap" in makeInfo.keys() else {}
        layerTaskSQLStrArrMap = {}
        for layer in layerArr:
            taskSQLStrArr = []
            for tableNumber in layerInfoArrMap[layer]:
                if tableNumber in tableNumberArr:
                    for interDependenceTableNumber in \
                            tableNumberInfoMap[tableNumber]["interDependence"] \
                                    if tableNumber in tableNumberInfoMap.keys() and "interDependence" in tableNumberInfoMap[tableNumber].keys() \
                                    else []:
                        if interDependenceTableNumber not in tableNumberArr:
                            tableNumberArr.append(interDependenceTableNumber)
                    makeGameNameArr = tableNumberInfoMap[tableNumber]["interDependenceGame"] \
                        if tableNumber in tableNumberInfoMap.keys() and "interDependenceGame" in tableNumberInfoMap[tableNumber].keys() \
                        else [gameName]
                    for makeGameName in makeGameNameArr:
                        eval(f"exec('from sql.{makeGameName}.{reportName}.ReportMain import ReportMain as {makeGameName}ReportMain')")
                        reportMain = eval(f" {makeGameName}ReportMain()")
                        reportType, reportSQLArray = eval(f"reportMain.insert{tableNumber}DataSQL({makeInfo})")
                        makeCopyInfo = copy.deepcopy(makeInfo)
                        makeCopyInfo['gameName'] = makeGameName
                        makeCopyInfo["sqlReplaceArr"] = []
                        for sqlReplace in makeInfo["sqlReplaceArr"]:
                            makeCopyInfo["sqlReplaceArr"].append(["[:GameName]", makeGameName] if sqlReplace[0] == "[:GameName]" else sqlReplace)
                        if reportType == "OrderInsert":  # 順序處理
                            reportSQLArr = self.__makeOrderInsertSQL(makeInfo, reportSQLArray)
                        elif reportType == "MutiInsert":  # 平行處理
                            reportSQLArr = self.__makeMutiInsertSQL(makeInfo, reportSQLArray)
                        elif reportType == "MoveModelExtract":  # 平行處理
                            reportSQLArr = self.__makeMoveModelExtractSQL(makeInfo, reportSQLArray)
                        elif reportType == "MoveModelExtractMuti":  # 平行處理
                            reportSQLArr = self.__makeMoveModelExtractMutiSQL(makeInfo, reportSQLArray)
                        elif reportType == "CreateBUReportPartitionUseME":  # 平行處理
                            reportSQLArr = self.__makeCreateBUReportPartitionUseME(makeInfo, reportSQLArray)
                        for reportSQL in reportSQLArr:
                            taskSQLStrArr.append(reportSQL)
            layerTaskSQLStrArrMap[layer] = taskSQLStrArr
        return layerTaskSQLStrArrMap

    def __makeOrderInsertSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "worldNameArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []
        # print(reportSQLArray)
        if reportSQLArray[0].find("[:World]") >= 0:
            for worldName in worldNameArr:
                reportSQLAll = ""
                for reportSQL in reportSQLArray:
                    for sqlReplace in sqlReplaceArr:
                        reportSQL = reportSQL.replace(sqlReplace[0], sqlReplace[1])
                    reportSQL = reportSQL.replace("[:World]", worldName)
                    reportSQLAll = reportSQLAll + reportSQL
                taskSQLStrArr.append(reportSQLAll)
        else:
            reportSQLAll = ""
            for reportSQL in reportSQLArray:
                for sqlReplace in sqlReplaceArr:
                    reportSQL = reportSQL.replace(sqlReplace[0], sqlReplace[1])
                reportSQLAll = reportSQLAll + reportSQL

            taskSQLStrArr.append(reportSQLAll)
        return taskSQLStrArr

    def __makeMutiInsertSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "worldNameArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []

        fromInitSQLCode = reportSQLArray[0]
        mutiInsertInitSQLCode = reportSQLArray[1]

        insertDataSQL = fromInitSQLCode
        for worldName in worldNameArr:
            insertDataSQL += mutiInsertInitSQLCode.replace("[:World]", worldName)

        for sqlReplace in sqlReplaceArr:
            insertDataSQL = insertDataSQL.replace(sqlReplace[0], sqlReplace[1])

        insertDataSQL = insertDataSQL + ";"
        taskSQLStrArr.append(insertDataSQL)
        return taskSQLStrArr

    def __makeMoveModelExtractSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "worldNameArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []

        initMoveModelExtractSQL = """
            INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DT]',world='[:World]',tablenumber='[:TableNumber]')
            SELECT
                AA.CommonData_1, AA.CommonData_2, AA.CommonData_3, AA.CommonData_4, AA.CommonData_5
                , AA.CommonData_6, AA.CommonData_7, AA.CommonData_8, AA.CommonData_9, AA.CommonData_10
                , AA.CommonData_11, AA.CommonData_12, AA.CommonData_13, AA.CommonData_14, AA.CommonData_15
                , AA.UniqueInt_1, AA.UniqueInt_2, AA.UniqueInt_3, AA.UniqueInt_4, AA.UniqueInt_5
                , AA.UniqueInt_6, AA.UniqueInt_7, AA.UniqueInt_8, AA.UniqueInt_9 , AA.UniqueInt_10
                , AA.UniqueInt_11, AA.UniqueInt_12, AA.UniqueInt_13, AA.UniqueInt_14, AA.UniqueInt_15
                , AA.UniqueStr_1 , AA.UniqueStr_2, AA.UniqueStr_3, AA.UniqueStr_4, AA.UniqueStr_5
                , AA.UniqueStr_6, AA.UniqueStr_7, AA.UniqueStr_8, AA.UniqueStr_9, AA.UniqueStr_10
                , AA.UniqueStr_11 , AA.uniquestr_12, AA.uniquestr_13, AA.uniquestr_14, AA.uniquestr_15
                , AA.uniquestr_16, AA.uniquestr_17, AA.uniquestr_18, AA.uniquestr_19, AA.uniquestr_20
                , AA.uniquedbl_1, AA.uniquedbl_2, AA.uniquedbl_3, AA.uniquedbl_4, AA.uniquedbl_5
                , AA.uniquedbl_6, AA.uniquedbl_7, AA.uniquedbl_8, AA.uniquedbl_9, AA.uniquedbl_10
                , AA.uniquedbl_11, AA.uniquedbl_12, AA.uniquedbl_13, AA.uniquedbl_14, AA.uniquedbl_15
                , AA.uniquedbl_16, AA.uniquedbl_17, AA.uniquedbl_18, AA.uniquedbl_19, AA.uniquedbl_20
                , AA.uniquetime_1, AA.uniquetime_2, AA.uniquetime_3
                , AA.otherstr_1, AA.otherstr_2, AA.otherstr_3, AA.otherstr_4, AA.otherstr_5
                , AA.otherstr_6, AA.otherstr_7, AA.otherstr_8, AA.otherstr_9, AA.otherstr_10
                , AA.uniquearray_1, AA.uniquearray_2, AA.uniquejson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DT]'
                AND AA.world= '[:World]'
                AND AA.tablenumber= '[:TableNumber]';
            """
        moveModelExtractSQL = initMoveModelExtractSQL.replace('[:GameName]',reportSQLArray[0])
        moveModelExtractSQL = moveModelExtractSQL.replace('[:DT]',reportSQLArray[1])
        moveModelExtractSQL = moveModelExtractSQL.replace('[:World]',reportSQLArray[2])
        moveModelExtractSQL = moveModelExtractSQL.replace('[:TableNumber]',reportSQLArray[3])

        if reportSQLArray[2] == "[:World]" :
            for worldName in worldNameArr:
                moveModelExtractSQL_World = moveModelExtractSQL.replace("[:World]", worldName)
                for sqlReplace in sqlReplaceArr:
                    moveModelExtractSQL_World = moveModelExtractSQL_World.replace(sqlReplace[0], sqlReplace[1])
                taskSQLStrArr.append(moveModelExtractSQL_World)
        else :
            for sqlReplace in sqlReplaceArr:
                moveModelExtractSQL = moveModelExtractSQL.replace(sqlReplace[0], sqlReplace[1])
            taskSQLStrArr.append(moveModelExtractSQL)
        return taskSQLStrArr

    def __makeMoveModelExtractMutiSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "worldNameArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []

        initFromSQLCode = """
            FROM (
                SELECT
                    AA.CommonData_1, AA.CommonData_2, AA.CommonData_3, AA.CommonData_4, AA.CommonData_5
                    , AA.CommonData_6, AA.CommonData_7, AA.CommonData_8, AA.CommonData_9, AA.CommonData_10
                    , AA.CommonData_11, AA.CommonData_12, AA.CommonData_13, AA.CommonData_14, AA.CommonData_15
                    , AA.UniqueInt_1, AA.UniqueInt_2, AA.UniqueInt_3, AA.UniqueInt_4, AA.UniqueInt_5
                    , AA.UniqueInt_6, AA.UniqueInt_7, AA.UniqueInt_8, AA.UniqueInt_9 , AA.UniqueInt_10
                    , AA.UniqueInt_11, AA.UniqueInt_12, AA.UniqueInt_13, AA.UniqueInt_14, AA.UniqueInt_15
                    , AA.UniqueStr_1 , AA.UniqueStr_2, AA.UniqueStr_3, AA.UniqueStr_4, AA.UniqueStr_5
                    , AA.UniqueStr_6, AA.UniqueStr_7, AA.UniqueStr_8, AA.UniqueStr_9, AA.UniqueStr_10
                    , AA.UniqueStr_11 , AA.uniquestr_12, AA.uniquestr_13, AA.uniquestr_14, AA.uniquestr_15
                    , AA.uniquestr_16, AA.uniquestr_17, AA.uniquestr_18, AA.uniquestr_19, AA.uniquestr_20
                    , AA.uniquedbl_1, AA.uniquedbl_2, AA.uniquedbl_3, AA.uniquedbl_4, AA.uniquedbl_5
                    , AA.uniquedbl_6, AA.uniquedbl_7, AA.uniquedbl_8, AA.uniquedbl_9, AA.uniquedbl_10
                    , AA.uniquedbl_11, AA.uniquedbl_12, AA.uniquedbl_13, AA.uniquedbl_14, AA.uniquedbl_15
                    , AA.uniquedbl_16, AA.uniquedbl_17, AA.uniquedbl_18, AA.uniquedbl_19, AA.uniquedbl_20
                    , AA.uniquetime_1, AA.uniquetime_2, AA.uniquetime_3
                    , AA.otherstr_1, AA.otherstr_2, AA.otherstr_3, AA.otherstr_4, AA.otherstr_5
                    , AA.otherstr_6, AA.otherstr_7, AA.otherstr_8, AA.otherstr_9, AA.otherstr_10
                    , AA.uniquearray_1, AA.uniquearray_2, AA.uniquejson_1
                    , AA.world
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = '[:GameName]'
                    AND AA.dt = '[:DT]'
                    AND AA.tablenumber= '[:TableNumber]'
            ) tmp """
        fromSQLCode = initFromSQLCode.replace('[:GameName]', reportSQLArray[0])
        fromSQLCode = fromSQLCode.replace('[:DT]', reportSQLArray[1])
        fromSQLCode = fromSQLCode.replace('[:TableNumber]', reportSQLArray[3])
        initMutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.business_bureport 
            PARTITION(game='[:GameName]',dt='[:DT]',world='[:World]',tablenumber='[:TableNumber]')
            SELECT CommonData_1, CommonData_2, CommonData_3, CommonData_4, CommonData_5
            , CommonData_6, CommonData_7, CommonData_8, CommonData_9, CommonData_10
            , CommonData_11, CommonData_12, CommonData_13, CommonData_14, CommonData_15
            , UniqueInt_1, UniqueInt_2, UniqueInt_3, UniqueInt_4, UniqueInt_5
            , UniqueInt_6, UniqueInt_7, UniqueInt_8, UniqueInt_9 , UniqueInt_10
            , UniqueInt_11, UniqueInt_12, UniqueInt_13, UniqueInt_14, UniqueInt_15
            , UniqueStr_1 , UniqueStr_2, UniqueStr_3, UniqueStr_4, UniqueStr_5
            , UniqueStr_6, UniqueStr_7, UniqueStr_8, UniqueStr_9, UniqueStr_10
            , UniqueStr_11 , uniquestr_12, uniquestr_13, uniquestr_14, uniquestr_15
            , uniquestr_16, uniquestr_17, uniquestr_18, uniquestr_19, uniquestr_20
            , uniquedbl_1, uniquedbl_2, uniquedbl_3, uniquedbl_4, uniquedbl_5
            , uniquedbl_6, uniquedbl_7, uniquedbl_8, uniquedbl_9, uniquedbl_10
            , uniquedbl_11, uniquedbl_12, uniquedbl_13, uniquedbl_14, uniquedbl_15
            , uniquedbl_16, uniquedbl_17, uniquedbl_18, uniquedbl_19, uniquedbl_20
            , uniquetime_1, uniquetime_2, uniquetime_3
            , otherstr_1, otherstr_2, otherstr_3, otherstr_4, otherstr_5
            , otherstr_6, otherstr_7, otherstr_8, otherstr_9, otherstr_10
            , uniquearray_1, uniquearray_2, uniquejson_1
            WHERE world = '[:World]' """
        mutiInsertSQLCode = initMutiInsertSQLCode.replace('[:GameName]', reportSQLArray[0])
        mutiInsertSQLCode = mutiInsertSQLCode.replace('[:DT]', reportSQLArray[1])
        mutiInsertSQLCode = mutiInsertSQLCode.replace('[:World]', reportSQLArray[2])
        mutiInsertSQLCode = mutiInsertSQLCode.replace('[:TableNumber]', reportSQLArray[3])

        moveModelExtractSQL = fromSQLCode
        if mutiInsertSQLCode.find("[:World]") >= 0:
            for worldName in worldNameArr:
                moveModelExtractSQL += mutiInsertSQLCode.replace("[:World]", worldName)
        else:
            moveModelExtractSQL += mutiInsertSQLCode

        for sqlReplace in sqlReplaceArr:
            moveModelExtractSQL = moveModelExtractSQL.replace(sqlReplace[0], sqlReplace[1])
        moveModelExtractSQL = moveModelExtractSQL + ";"
        taskSQLStrArr.append(moveModelExtractSQL)
        return taskSQLStrArr

    def __makeCreateBUReportPartitionUseME(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "worldNameArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []

        initDropBUReportPartitionUseMESQL = """
            ALTER TABLE gtwpd.business_bureport Drop IF EXISTS
            PARTITION(game='[:GameName]',dt='[:DT]',world='[:World]',tablenumber='[:TableNumber]') ;"""
        initCreateBUReportPartitionUseMESQL = """
            ALTER TABLE gtwpd.business_bureport ADD IF NOT EXISTS
            PARTITION(game='[:GameName]',dt='[:DT]',world='[:World]',tablenumber='[:TableNumber]')
            LOCATION '/user/GTW_PD/DB/ModelExtract/modelextract/game=[:GameName]/dt=[:DT]/world=[:World]/tablenumber=[:TableNumber]' ;"""
        initSetBUReportPartitionUseMESQL = """
            ALTER TABLE gtwpd.business_bureport 
            PARTITION(game='[:GameName]',dt='[:DT]',world='[:World]',tablenumber='[:TableNumber]')
            SET LOCATION '/user/GTW_PD/DB/ModelExtract/modelextract/game=[:GameName]/dt=[:DT]/world=[:World]/tablenumber=[:TableNumber]' ;"""

        dropBUReportPartitionUseMESQL = initDropBUReportPartitionUseMESQL.replace('[:GameName]', reportSQLArray[0])
        dropBUReportPartitionUseMESQL = dropBUReportPartitionUseMESQL.replace('[:DT]', reportSQLArray[1])
        dropBUReportPartitionUseMESQL = dropBUReportPartitionUseMESQL.replace('[:World]', reportSQLArray[2])
        dropBUReportPartitionUseMESQL = dropBUReportPartitionUseMESQL.replace('[:TableNumber]', reportSQLArray[3])

        createBUReportPartitionUseMESQL = initCreateBUReportPartitionUseMESQL.replace('[:GameName]', reportSQLArray[0])
        createBUReportPartitionUseMESQL = createBUReportPartitionUseMESQL.replace('[:DT]', reportSQLArray[1])
        createBUReportPartitionUseMESQL = createBUReportPartitionUseMESQL.replace('[:World]', reportSQLArray[2])
        createBUReportPartitionUseMESQL = createBUReportPartitionUseMESQL.replace('[:TableNumber]', reportSQLArray[3])

        if reportSQLArray[2] == "[:World]":
            for worldName in worldNameArr:
                dropBUReportPartitionUseMESQL_World = dropBUReportPartitionUseMESQL.replace("[:World]", worldName)
                createBUReportPartitionUseMESQL_World = createBUReportPartitionUseMESQL.replace("[:World]", worldName)
                for sqlReplace in sqlReplaceArr:
                    dropBUReportPartitionUseMESQL_World = dropBUReportPartitionUseMESQL_World.replace(sqlReplace[0], sqlReplace[1])
                    createBUReportPartitionUseMESQL_World = createBUReportPartitionUseMESQL_World.replace(sqlReplace[0], sqlReplace[1])
                sqlStrArr = dropBUReportPartitionUseMESQL_World + createBUReportPartitionUseMESQL_World
                taskSQLStrArr.append(sqlStrArr)
        else:
            for sqlReplace in sqlReplaceArr:
                dropBUReportPartitionUseMESQL = dropBUReportPartitionUseMESQL.replace(sqlReplace[0], sqlReplace[1])
                createBUReportPartitionUseMESQL = createBUReportPartitionUseMESQL.replace(sqlReplace[0], sqlReplace[1])

            sqlStrArr = dropBUReportPartitionUseMESQL + createBUReportPartitionUseMESQL
            taskSQLStrArr.append(sqlStrArr)
        return taskSQLStrArr

