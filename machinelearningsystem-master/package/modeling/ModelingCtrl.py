import os
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from dotenv import load_dotenv
from package.common.database.sqlTool import SqlTool
import package.common.common.GamaniaDateTime as gamaniaDateTime
import datetime
import calendar
import time
import copy
import pandas as pd
import numpy as np

sqlTool = SqlTool()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 200000000)


class ModelingCtrl:

    def __init__(self):
        load_dotenv(dotenv_path="env/hive.env")
        self.hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        load_dotenv(dotenv_path="env/hdfs.env")
        self.hdfsCtrl = HDFSCtrl(
            url=os.getenv("HDFS_HOST")
            , user=os.getenv("HDFS_USER")
            , password=os.getenv("HDFS_PASSWD")
            , filePath=os.getenv("HDFS_PATH")
        )

    def makeModeling(self, modelInfo):
        modelInfo["result"] = {}
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        rawdataVersion = modelInfo["rawdataVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        usemodelVersion = modelInfo["usemodelVersion"]
        makeTime = modelInfo["makeTime"]
        runStepArr = modelInfo["runStepArr"]
        # hiveCtrl = modelInfo["hiveCtrl"]
        print("start make model , version is {} , date is {}".format(modelVersion, makeTime))

        if rawdataVersion != None and rawdataVersion[0] == "R" and "rawdata" in runStepArr:
            print("  start make rawdata , version is {} , date is {}".format(rawdataVersion,makeTime))
            eval(f"exec('from {productName}.{project}.modeling.RawData_{project} import RawData_{project} as {productName}_{project}_RawDataMain')")
            rawDataMain = eval(f" {productName}_{project}_RawDataMain()")
            rawDataType, rawDataObject,rawDataRestlt = eval(f"rawDataMain.MakeRawData_{project}_{rawdataVersion}({modelInfo})")
            modelInfo["result"]["rawdata"] = rawDataRestlt
            print("    rawData {} type is {}".format(rawdataVersion, rawDataType))
            if rawDataType == "MakeRawDataCtrl":
                self.__makeRawDataCtrl(modelInfo, rawDataObject)
            elif rawDataType == "MakeRawDataOrderSQLInsert":
                self.__makeRawDataOrderSQLInsert(modelInfo, rawDataObject)
            elif rawDataType == "MakeRawDataFileInsert":
                self.__makeRawDataFileInsert(modelInfo, rawDataObject,overwrite=False)
            elif rawDataType == "MakeRawDataFileInsertOverwrite":
                self.__makeRawDataFileInsert(modelInfo, rawDataObject,overwrite=True)
            elif rawDataType == "MakeRawDataFreeFuction":
                self.__makeRawDataFreeFuction(modelInfo)
            print("  end make rawdata , version is {} , date is {}".format(rawdataVersion,makeTime))

        if preprocessVersion != None and preprocessVersion[0] == "P" and "preprocess" in runStepArr:
            print("  start make preprocess , version is {} , date is {}".format(preprocessVersion,makeTime))
            eval(f"exec('from {productName}.{project}.modeling.PreProcess_{project} import PreProcess_{project} as {productName}_{project}_PreProcessMain')")
            preprocessMain = eval(f" {productName}_{project}_PreProcessMain()")
            preprocessType, preprocessObject ,preprocessRestlt = eval(f"preprocessMain.MakePreProcess_{project}_{preprocessVersion}({modelInfo})")
            modelInfo["result"]["preprocess"] = preprocessRestlt
            print("    preprocess {} type is {}".format(preprocessVersion, preprocessType))
            if preprocessType == "MakePreProcessCtrl":
                self.__makePreProcessCtrl(modelInfo, preprocessObject)
            elif preprocessType == "MakePreProcessOrderSQLInsert":
                self.__makePreProcessOrderSQLInsert(modelInfo, preprocessObject)
            elif preprocessType == "MakePreProcessFileInsert":
                self.__makePreProcessFileInsert(modelInfo, preprocessObject,overwrite=False)
            elif preprocessType == "MakePreProcessFileInsertOverwrite":
                self.__makePreProcessFileInsert(modelInfo, preprocessObject,overwrite=True)
            elif preprocessType == "MakePreProcessFreeFuction":
                self.__makePreProcessFreeFuction(modelInfo)
            print("  end make preprocess , version is {} , date is {}".format(preprocessVersion,makeTime))

        if usemodelVersion != None and usemodelVersion[0] == "M" and "usemodel" in runStepArr:
            print("  start make usemodel , version is {} , date is {}".format(usemodelVersion,makeTime))
            eval(f"exec('from {productName}.{project}.modeling.UseModel_{project} import UseModel_{project} as {productName}_{project}_UseModelMain')")
            useModelMain = eval(f" {productName}_{project}_UseModelMain()")
            useModelType, useModelObject , useModelRestlt = eval(f"useModelMain.MakeUseModel_{project}_{usemodelVersion}({modelInfo})")
            modelInfo["result"]["usemodel"] = useModelRestlt
            print("    usemodel {} type is {}".format(usemodelVersion, useModelType))
            if useModelType == "MakeUseModelOrderSQLInsert":
                self.__makeUseModelOrderSQLInsert(modelInfo, useModelObject)
            elif useModelType == "MakeUseModelFileInsert":
                self.__makeUseModelFileInsert(modelInfo, useModelObject,overwrite=False)
            elif useModelType == "MakeUseModelFileInsertOverwrite":
                self.__makeUseModelFileInsert(modelInfo, useModelObject,overwrite=True)
            elif useModelType == "MakeUseModelFreeFuction":
                self.__makeUseModelFreeFuction(modelInfo)
            print("  end make usemodel , version is {} , date is {}".format(usemodelVersion, makeTime))

        print("end make model , version is {} , date is {}".format(modelVersion, makeTime))

    # ====================================================================================================

    def __makeRawDataCtrl(self, modelInfo, rawDataStr):
        runtype = modelInfo["runType"]
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        # preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        # makeTime = modelInfo["makeTime"]
        # stepArr = modelInfo["stepArr"]

        localFilePath = f'{productName}/{project}/file/ctrl/{productName}_{rawDataStr}_RawDataCtrl.csv'
        remoteFilePath = f'/user/GTW_PD/File/MachineLearningSystem/product={productName}/project={project}/step=RawData/version={rawdataVersion}/{productName}_{rawDataStr}_RawDataCtrl.csv'
        if runtype == 'buildmodel' :
            self.hdfsCtrl.upload(remoteFilePath, localFilePath)
            modelInfo["rawDataInfoCtrlTable"] = pd.read_csv(localFilePath, dtype='str', na_filter=False)
        else :
            self.hdfsCtrl.download(remoteFilePath, localFilePath)
            modelInfo["rawDataInfoCtrlTable"] = pd.read_csv(localFilePath, dtype='str', na_filter=False)

        modelInfo = self.__makeRawDataCtrlSQL(modelInfo)
        rawDataSQLList = modelInfo["rawDataSQLList"]
        for rawDataSQL in rawDataSQLList :
            self.hiveCtrl.executeSQL_TCByCount(rawDataSQL,3)
        return modelInfo

    def __makeRawDataOrderSQLInsert(self, modelInfo, rawDataSQLList):
        modelInfo["rawDataSQLList"] = rawDataSQLList
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        modelVersion = modelInfo["modelVersion"]
        usemodelVersion = modelInfo["usemodelVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d")
        firstMonthTime = gamaniaDateTime.getThisMonthFirstDate(makeTime)
        lastMonthTime = gamaniaDateTime.getThisMonthLastDate(makeTime)

        sqlReplaceArr = [
            ["[:ProductName]", productName]
            , ["[:Project]", project]
            , ["[:ModelVersion]", modelVersion]
            , ["[:UseModelVersion]", usemodelVersion]
            , ["[:PreProcessVersion]", preprocessVersion]
            , ["[:RawDataVersion]", rawdataVersion]
            , ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
            , ["[:DateLineFirstMonth]", firstMonthTime.strftime("%Y-%m-%d")]
            , ["[:DateLineLastMonth]", lastMonthTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLineFirstMonth]", firstMonthTime.strftime("%Y%m%d")]
            , ["[:DateNoLineLastMonth]", lastMonthTime.strftime("%Y%m%d")]
        ]
        for rawDataSQL in rawDataSQLList :
            for sqlReplace in sqlReplaceArr :
                if type(sqlReplace[1]) == type('str') :
                    rawDataSQL = rawDataSQL.replace(sqlReplace[0],sqlReplace[1])
            sqlStrArr = rawDataSQL.split(";")[:-1]
            for sqlStr in sqlStrArr:
                print(sqlStr)
                # 若SQL中有分號，需先取代為[:SEMICOLON]
                sqlStr = sqlStr.replace("[:SEMICOLON]", ";")
                self.hiveCtrl.executeSQL_TCByCount(sqlStr,3)
        return modelInfo

    def __makeRawDataFileInsert(self, modelInfo, rawDataDF,overwrite=True):
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        # preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = modelInfo["makeTime"]
        # stepArr = modelInfo["stepArr"]

        # makeUseDataDFToHDFSHive (usedataDF,productName,project,version,dt,step) 相關輸入方式
        self.makeUseDataDFToHDFSHive(rawDataDF, productName, project, rawdataVersion, datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d"),  "RawData",overwrite)

    def __makeRawDataFreeFuction(self, modelInfo):
        return modelInfo

    # ====================================================================================================

    def __makePreProcessCtrl(self, modelInfo, preProcessStr):
        runtype = modelInfo["runType"]
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        # rawdataVersion = modelInfo["rawdataVersion"]
        # makeTime = modelInfo["makeTime"]
        # stepArr = modelInfo["stepArr"]

        localFilePath = f'{productName}/{project}/file/ctrl/{productName}_{preProcessStr}_PreProcessCtrl.csv'
        remoteFilePath = f'/user/GTW_PD/File/MachineLearningSystem/product={productName}/project={project}/step=PreProcess/version={preprocessVersion}/{productName}_{preProcessStr}_RawDataCtrl.csv'
        if runtype == 'buildmodel':
            self.hdfsCtrl.upload(remoteFilePath, localFilePath)
            modelInfo["preProcessInfoCtrlTable"] = pd.read_csv(localFilePath, dtype='str', na_filter=False)
        else:
            self.hdfsCtrl.download(remoteFilePath, localFilePath)
            modelInfo["preProcessInfoCtrlTable"] = pd.read_csv(localFilePath, dtype='str', na_filter=False)

        modelInfo = self.__makePreProcessCtrlSQL(modelInfo)

        preProcessSQLList = modelInfo["preProcessSQLList"]
        for preProcessSQL in preProcessSQLList:
            self.hiveCtrl.executeSQL_TCByCount(preProcessSQL, 3)
        return modelInfo

    def __makePreProcessOrderSQLInsert(self, modelInfo, preProcessSQLList):
        modelInfo["preProcessSQLList"] = preProcessSQLList
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        modelVersion = modelInfo["modelVersion"]
        usemodelVersion = modelInfo["usemodelVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d")
        firstMonthTime = gamaniaDateTime.getThisMonthFirstDate(makeTime)
        lastMonthTime = gamaniaDateTime.getThisMonthLastDate(makeTime)

        sqlReplaceArr = [
            ["[:ProductName]", productName]
            , ["[:Project]", project]
            , ["[:ModelVersion]", modelVersion]
            , ["[:UseModelVersion]", usemodelVersion]
            , ["[:PreProcessVersion]", preprocessVersion]
            , ["[:RawDataVersion]", rawdataVersion]
            , ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
            , ["[:DateLineFirstMonth]", firstMonthTime.strftime("%Y-%m-%d")]
            , ["[:DateLineLastMonth]", lastMonthTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLineFirstMonth]", firstMonthTime.strftime("%Y%m%d")]
            , ["[:DateNoLineLastMonth]", lastMonthTime.strftime("%Y%m%d")]
        ]

        for preProcessSQL in preProcessSQLList:
            for sqlReplace in sqlReplaceArr :
                if type(sqlReplace[1]) == type('str') :
                    preProcessSQL = preProcessSQL.replace(sqlReplace[0],sqlReplace[1])
            sqlStrArr = preProcessSQL.split(";")[:-1]
            for sqlStr in sqlStrArr:
                print(sqlStr)
                # 若SQL中有分號，需先取代為[:SEMICOLON]
                sqlStr = sqlStr.replace("[:SEMICOLON]", ";")
                self.hiveCtrl.executeSQL_TCByCount(sqlStr,3)
        return modelInfo

    def __makePreProcessFileInsert(self, modelInfo, preProcessDF,overwrite=True):
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        # rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = modelInfo["makeTime"]
        # stepArr = modelInfo["stepArr"]

        # makeUseDataDFToHDFSHive (usedataDF,productName,project,version,dt,step) 相關輸入方式
        self.makeUseDataDFToHDFSHive(preProcessDF, productName, project, preprocessVersion, datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d"), "PreProcess",overwrite)
        return modelInfo

    def __makePreProcessFreeFuction(self, modelInfo):
        return modelInfo

    # ====================================================================================================
    def __makeUseModelOrderSQLInsert(self, modelInfo, useModelSQLList):
        modelInfo["useModelSQLList"] = useModelSQLList
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        modelVersion = modelInfo["modelVersion"]
        usemodelVersion = modelInfo["usemodelVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d")
        firstMonthTime = gamaniaDateTime.getThisMonthFirstDate(makeTime)
        lastMonthTime = gamaniaDateTime.getThisMonthLastDate(makeTime)

        sqlReplaceArr = [
            ["[:ProductName]", productName]
            , ["[:Project]", project]
            , ["[:ModelVersion]", modelVersion]
            , ["[:UseModelVersion]", usemodelVersion]
            , ["[:PreProcessVersion]", preprocessVersion]
            , ["[:RawDataVersion]", rawdataVersion]
            , ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
            , ["[:DateLineFirstMonth]", firstMonthTime.strftime("%Y-%m-%d")]
            , ["[:DateLineLastMonth]", lastMonthTime.strftime("%Y-%m-%d")]
            , ["[:DateNoLineFirstMonth]", firstMonthTime.strftime("%Y%m%d")]
            , ["[:DateNoLineLastMonth]", lastMonthTime.strftime("%Y%m%d")]
        ]
        for useModelSQL in useModelSQLList:
            for sqlReplace in sqlReplaceArr :
                if type(sqlReplace[1]) == type('str') :
                    useModelSQL = useModelSQL.replace(sqlReplace[0],sqlReplace[1])
            sqlStrArr = useModelSQL.split(";")[:-1]
            for sqlStr in sqlStrArr:
                # 若SQL中有分號，需先取代為[:SEMICOLON]
                print(sqlStr)
                sqlStr = sqlStr.replace("[:SEMICOLON]", ";")
                self.hiveCtrl.executeSQL_TCByCount(sqlStr,3)
        return modelInfo

    def __makeUseModelFileInsert(self, modelInfo, useModelDFArr,overwrite=True):
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        usemodelVersion = modelInfo["usemodelVersion"]
        # preprocessVersion = modelInfo["preprocessVersion"]
        # rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = modelInfo["makeTime"]
        # stepArr = modelInfo["stepArr"]

        # makeUseDataDFToHDFSHive (usedataDF,productName,project,version,dt,step) 相關輸入方式

        useModelDF = useModelDFArr[0]
        modelResultDF = useModelDFArr[0]
        modelScoreDF = useModelDFArr[1]
        self.makeUseDataDFToHDFSHive(useModelDF, productName, project, usemodelVersion, datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d"), "UseModel",overwrite)
        self.makeUseDataDFToHDFSHive(modelResultDF, productName, project, modelVersion, datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d"), "ModelResult",overwrite)
        self.makeUseDataDFToHDFSHive(modelScoreDF, productName, project, modelVersion, datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d"), "ModelScore",overwrite)
        return modelInfo


    def __makeUseModelFreeFuction(self, modelInfo):
        return modelInfo

    # ====================================================================================================

    # __makeRawDataCtrlFile ----------------------------------------------------------------------------------------------------

    def __makeRawDataCtrlSQL(self, modelInfo):
        modelInfo["withTableSQLList"] = self.__makeRawDataCtrlWithSQL(modelInfo)
        modelInfo["mainTableSQL"] = self.__makeRawDataCtrlMainSQL(modelInfo)
        rawDataSQLList = []
        rawDataSQLList.append(self.__makeRawDataCtrlFinalSQL(modelInfo))
        modelInfo["rawDataSQLList"] = rawDataSQLList
        return modelInfo

    def __makeRawDataCtrlFinalSQL(self, modelInfo):
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        # preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = modelInfo["makeTime"]
        dt = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        withTableSQLList = modelInfo["withTableSQLList"]
        mainTableSQL = modelInfo["mainTableSQL"]

        return """
WITH maintable AS (
[:WithTableSQL]
) INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:productName]' , project = '[:project]' , step = 'RawData' , version = '[:rawdataVersion]' , dt = '[:dt]' )
[:mainTableSQL]
        """.replace("[:WithTableSQL]", ' \n    UNION ALL \n    '.join(withTableSQLList)) \
            .replace("[:productName]", productName) \
            .replace("[:project]", project) \
            .replace("[:rawdataVersion]", rawdataVersion) \
            .replace("[:dt]", dt) \
            .replace("[:mainTableSQL]", mainTableSQL)

    def __makeRawDataCtrlWithSQL(self, modelInfo):
        # 參數讀取
        infoCtrlTable = modelInfo["rawDataInfoCtrlTable"].copy()
        productName = modelInfo["productName"]
        startDate = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        endDate = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        withTableSQLList = []

        # 根據來源數據決定相關內容
        for tempTableName in infoCtrlTable['origin_table'].unique():
            tempTable = infoCtrlTable[infoCtrlTable['origin_table'] == tempTableName]

            # 篩選對於commonData的額外條件
            condtions = ''
            for ind in range(len(tempTable)):
                if tempTable['new_name'].iloc[ind].lower().find('uniquefloat') < 0 and tempTable['conditions'].iloc[ind] != '':
                    condtions += f"\n        AND {tempTable['conditions'].iloc[ind]}"

            # 將所有的欄位名稱轉換成字串，以\n + 逗點連接
            newcol = tempTable['is_new'] == 'T'
            tempTable['origin_col'][newcol] = tempTable['origin_col'][newcol] + ' AS ' + tempTable['new_name'][newcol]
            col_names = [x for x in tempTable['origin_col'].unique() if x != '']  # 去除重複
            colStr = '\n        , '.join(col_names)  # 轉成字串

            # 將所有的tableNumber轉換成字串，以逗點連接
            tableNumberList = [x for x in tempTable['tablenumber'].unique() if x != '']  # 去除重複
            tableNumberStr = ','.join(tableNumberList)  # 轉成字串

            # 將所有參數組合為SQL
            withTableSQL =  """
    SELECT   
        [:colStr]         
        , tablenumber  
    FROM [:tableName]
    WHERE 1 = 1  
        AND game = '[:productName]'
        AND dt >= [:startDate] 
        AND dt <= [:endDate] 
        AND tablenumber IN ( [:tableNumberStr] )
        [:condtions]
"""         .replace("[:colStr]",colStr) \
            .replace("[:tableName]",tempTableName) \
            .replace("[:productName]",productName) \
            .replace("[:startDate]",startDate) \
            .replace("[:endDate]",endDate) \
            .replace("[:tableNumberStr]",tableNumberStr) \
            .replace("[:condtions]", condtions)

            withTableSQLList.append(withTableSQL)

        return withTableSQLList

        # 主查詢部分的SQL

    def __makeRawDataCtrlMainSQL(self, modelInfo):
        infoCtrlTable = modelInfo["rawDataInfoCtrlTable"].copy()
        newcol = infoCtrlTable['is_new'] == 'T'
        infoCtrlTable['origin_col'][newcol] = infoCtrlTable['new_name'][newcol]
        resultList = []
        group_cols = []
        UniqueFloatcnt = 0  # 紀錄資料欄數
        for i in range(len(infoCtrlTable)):
            tmp = infoCtrlTable.iloc[i]
            if tmp['func'] == '':
                continue
            elif tmp['func'] == 'groupby':
                s = f"{tmp['origin_col']} AS {tmp['new_name']}"  # 列在group by 欄位的處理
                group_cols.append(tmp['origin_col'])
            elif tmp['func'] == 'common':  # 沒列在group by 欄位，但是在common的處理
                s = f"MAX ( {tmp['origin_col']} ) AS {tmp['new_name']}"
            else:
                # 所有建模相關之其他資料
                UniqueFloatcnt += 1  # 資料欄數累加器
                condition = '' if tmp['conditions'] == '' else f" AND {tmp['conditions']}"  # 是否有額外條件
                if tmp['func'] == 'COUNTD':
                    s = f"COUNT ( DISTINCT ( CASE WHEN tablenumber = {tmp['tablenumber']}{condition} " \
                        f"THEN {tmp['then_value']} ELSE {tmp['else_value']} END) ) AS {tmp['new_name']}"
                else:
                    s = f"{tmp['func']} ( CASE WHEN tablenumber = {tmp['tablenumber']}{condition} " \
                        f"THEN {tmp['then_value']} ELSE {tmp['else_value']} END ) AS {tmp['new_name']}"
            resultList.append(s)  # 建立出所有欄位的List

        for i in range(UniqueFloatcnt + 1, 200 + 1):
            resultList.append(f'NULL AS UniqueFloat_{i:03}')  # 如果資料欄數未滿200 則補空到200
        resultList.append(f'NULL AS UniqueJson_001')  # 如果資料欄數未滿200 則補空到200
        colStr = '\n    , '.join(resultList)  # 將欄位的List 轉成字串
        groupStr = '\n    , '.join(group_cols)

        return f"""SELECT \n    {colStr} \nFROM maintable \nGROUP BY \n    {groupStr}"""

        # 最後上下合併的SQL

    # __makeRawDataOrderSQLInsert ----------------------------------------------------------------------------------------------------
    # No Other Function

    # __makeRawDataFileInsert ----------------------------------------------------------------------------------------------------
    # No Other Function

    # __makeRawDataFreeFuction ----------------------------------------------------------------------------------------------------
    # No Other Function

    # __makePreProcessCtrlFile ----------------------------------------------------------------------------------------------------

    def __makePreProcessCtrlSQL(self, modelInfo) :
        modelInfo["withTableSQL1"] , modelInfo["withTableSQL2"] = self.__makePreProcessCtrlWithSQL(modelInfo)
        modelInfo["mainTableSQL"] = self.__makePreProcessCtrlMainSQL(modelInfo)
        preProcessSQLList = []
        preProcessSQLList.append(self.__makePreProcessCtrlFinalSQL(modelInfo))
        modelInfo["preProcessSQLList"] = preProcessSQLList
        return modelInfo

    def __makePreProcessCtrlFinalSQL(self, modelInfo):
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        preprocessVersion = modelInfo["preprocessVersion"]
        # rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = modelInfo["makeTime"]
        dt = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        withTableSQL_1 = modelInfo["withTableSQL1"]
        withTableSQL_2 = modelInfo["withTableSQL2"]
        mainTableSQL = modelInfo["mainTableSQL"]

        return """
WITH maintable1 AS ( [:WithTableSQL_1] ), maintable2 AS ( [:WithTableSQL_2] ) INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:productName]' , project = '[:project]' , step = 'PreProcess' , version = '[:rawdataVersion]' , dt = '[:dt]' ) [:mainTableSQL]
    """ .replace("[:WithTableSQL_1]", withTableSQL_1) \
        .replace("[:WithTableSQL_2]", withTableSQL_2) \
        .replace("[:mainTableSQL]", mainTableSQL) \
        .replace("[:productName]", productName) \
        .replace("[:project]", project) \
        .replace("[:rawdataVersion]", preprocessVersion) \
        .replace("[:dt]", dt)

    def __makePreProcessCtrlWithSQL(self, modelInfo):

        infoCtrlTable = modelInfo["preProcessInfoCtrlTable"].copy()
        productName = modelInfo["productName"]
        project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        # preprocessVersion = modelInfo["preprocessVersion"]
        rawdataVersion = modelInfo["rawdataVersion"]
        makeTime = modelInfo["makeTime"]
        dt = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        #withTableSQLList = modelInfo["withTableSQLList"]
        #mainTableSQL = modelInfo["mainTableSQL"]

        __commondataLength = 15
        __uniqueStrLength = 200
        __digit = 6

        with1_cloumn = []
        # commondata的部分
        for i in range(__commondataLength):
            with1_cloumn.append(f'CommonData_{str(i + 1).zfill(3)}')
        for i in range(len(infoCtrlTable)):
            tb = infoCtrlTable.iloc[i]
            colStr = tb['origin_col']
            logStr = tb['do_log'].split(' ')
            logDict = {1: [logStr[0], 'exp(1)'], 2: logStr}
            base = logDict[len(logStr)][1]
            if logDict[len(logStr)][0] == 'dblog':
                colStr = f"CASE WHEN ({colStr}) < 0 THEN -LOG({base}, -{colStr} + 1) ELSE LOG({base}, {colStr} + 1 ) END "
            else:
                if tb['negtive'] == 'zero':
                    colStr = f"CASE WHEN ({colStr}) < 0 THEN 0 ELSE ({colStr}) END"
                if logStr[0] == 'log':
                    colStr = f"LOG({base}, {colStr} +1 ) "
            with1_cloumn.append(f"{colStr} AS {tb['new_col']}")

        withTableSQL_1 = """
    SELECT   
        AA.[:colStr]         
    FROM gtwpd.model_usedata AA 
    WHERE 1 = 1  
        AND AA.product = '[:productName]'
        AND AA.project = '[:project]'
        AND AA.step = 'RawData'  
        AND AA.version = '[:rawdataVersion]'
        AND AA.dt = '[:dt]'
"""     .replace("[:colStr]",'\n        , AA.'.join(with1_cloumn)) \
        .replace("[:productName]",productName) \
        .replace("[:project]",project) \
        .replace("[:rawdataVersion]",rawdataVersion) \
        .replace("[:dt]",dt)


        with2_cloumn = []
        # commondata的部分
        for i in range(__uniqueStrLength):
            if i < len(infoCtrlTable):
                tb = infoCtrlTable.iloc[i]
                col = tb['new_col']
                dictScale = {'norm': [f'MIN({col})', f'MAX({col}) - MIN({col})'],
                             'std': [f'AVG({col})', f'stddev_samp({col})']
                             }
                # 預設不做scale
                scales = dictScale.setdefault(tb['scale'], ['MAX(0)', 'MAX(1)'])
                if tb['centering'] != 'T':
                    scales[0] = 'MAX(0)'
                with2_cloumn.append(f'{scales[0]} AS c_{col} \n        , {scales[1]} AS v_{col}')

        withTableSQL_2 = """
    SELECT
        [:colStr] 
    FROM maintable1
""".replace("[:colStr]", '\n        , '.join(with2_cloumn))

        return withTableSQL_1 , withTableSQL_2

    def __makePreProcessCtrlMainSQL(self, modelInfo):
        infoCtrlTable = modelInfo["preProcessInfoCtrlTable"].copy()
        # productName = modelInfo["productName"]
        # project = modelInfo["project"]
        # modelVersion = modelInfo["modelVersion"]
        # modelVersionFullCode = modelInfo["modelVersionFullCode"]
        # usemodelVersion = modelInfo["usemodelVersion"]
        # preprocessVersion = modelInfo["preprocessVersion"]
        # rawdataVersion = modelInfo["rawdataVersion"]
        # makeTime = modelInfo["makeTime"]
        # dt = datetime.datetime.strptime(modelInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        # withTableSQLList = modelInfo["withTableSQLList"]
        # mainTableSQL = modelInfo["mainTableSQL"]

        __commondataLength = 15
        __uniqueStrLength = 200
        __digit = 6

        main_cloumn = []
        # commondata的部分
        for i in range(__commondataLength):
            main_cloumn.append(f'CommonData_{str(i + 1).zfill(3)}')
        for i in range(__uniqueStrLength):
            if i < len(infoCtrlTable):
                col = infoCtrlTable['new_col'].iloc[i]
                main_cloumn.append(f'NVL(ROUND(({col} - c_{col}) / v_{col} ,{__digit}), 0) AS {col}')
            else:
                main_cloumn.append(f'NULL AS UniqueFloat_{i + 1:03}')  # 如果資料欄數未滿200 則補空到200

        mainTableSQL = """
SELECT
    [:colStr]
    , NULL AS UniqueJson_001
FROM maintable1 
JOIN maintable2
""".replace("[:colStr]", '\n    , '.join(main_cloumn))

        return mainTableSQL

    # __makePreProcessOrderSQLInsert ----------------------------------------------------------------------------------------------------
    # No Other Function

    # __makePreProcessFileInsert ----------------------------------------------------------------------------------------------------
    # No Other Function

    # __makePreProcessFreeFuction ----------------------------------------------------------------------------------------------------
    # No Other Function

    def makeUseDataDFToHDFSHive (self,usedataDF,productName,project,version,dt,step,overwrite=True):
        columns = [
            "commondata_001", "commondata_002", "commondata_003", "commondata_004", "commondata_005"
            , "commondata_006", "commondata_007", "commondata_008", "commondata_009", "commondata_010"
            , "commondata_011", "commondata_012", "commondata_013", "commondata_014", "commondata_015"
            , "uniquefloat_001", "uniquefloat_002", "uniquefloat_003", "uniquefloat_004", "uniquefloat_005"
            , "uniquefloat_006", "uniquefloat_007", "uniquefloat_008", "uniquefloat_009", "uniquefloat_010"
            , "uniquefloat_011", "uniquefloat_012", "uniquefloat_013", "uniquefloat_014", "uniquefloat_015"
            , "uniquefloat_016", "uniquefloat_017", "uniquefloat_018", "uniquefloat_019", "uniquefloat_020"
            , "uniquefloat_021", "uniquefloat_022", "uniquefloat_023", "uniquefloat_024", "uniquefloat_025"
            , "uniquefloat_026", "uniquefloat_027", "uniquefloat_028", "uniquefloat_029", "uniquefloat_030"
            , "uniquefloat_031", "uniquefloat_032", "uniquefloat_033", "uniquefloat_034", "uniquefloat_035"
            , "uniquefloat_036", "uniquefloat_037", "uniquefloat_038", "uniquefloat_039", "uniquefloat_040"
            , "uniquefloat_041", "uniquefloat_042", "uniquefloat_043", "uniquefloat_044", "uniquefloat_045"
            , "uniquefloat_046", "uniquefloat_047", "uniquefloat_048", "uniquefloat_049", "uniquefloat_050"
            , "uniquefloat_051", "uniquefloat_052", "uniquefloat_053", "uniquefloat_054", "uniquefloat_055"
            , "uniquefloat_056", "uniquefloat_057", "uniquefloat_058", "uniquefloat_059", "uniquefloat_060"
            , "uniquefloat_061", "uniquefloat_062", "uniquefloat_063", "uniquefloat_064", "uniquefloat_065"
            , "uniquefloat_066", "uniquefloat_067", "uniquefloat_068", "uniquefloat_069", "uniquefloat_070"
            , "uniquefloat_071", "uniquefloat_072", "uniquefloat_073", "uniquefloat_074", "uniquefloat_075"
            , "uniquefloat_076", "uniquefloat_077", "uniquefloat_078", "uniquefloat_079", "uniquefloat_080"
            , "uniquefloat_081", "uniquefloat_082", "uniquefloat_083", "uniquefloat_084", "uniquefloat_085"
            , "uniquefloat_086", "uniquefloat_087", "uniquefloat_088", "uniquefloat_089", "uniquefloat_090"
            , "uniquefloat_091", "uniquefloat_092", "uniquefloat_093", "uniquefloat_094", "uniquefloat_095"
            , "uniquefloat_096", "uniquefloat_097", "uniquefloat_098", "uniquefloat_099", "uniquefloat_100"
            , "uniquefloat_101", "uniquefloat_102", "uniquefloat_103", "uniquefloat_104", "uniquefloat_105"
            , "uniquefloat_106", "uniquefloat_107", "uniquefloat_108", "uniquefloat_109", "uniquefloat_110"
            , "uniquefloat_111", "uniquefloat_112", "uniquefloat_113", "uniquefloat_114", "uniquefloat_115"
            , "uniquefloat_116", "uniquefloat_117", "uniquefloat_118", "uniquefloat_119", "uniquefloat_120"
            , "uniquefloat_121", "uniquefloat_122", "uniquefloat_123", "uniquefloat_124", "uniquefloat_125"
            , "uniquefloat_126", "uniquefloat_127", "uniquefloat_128", "uniquefloat_129", "uniquefloat_130"
            , "uniquefloat_131", "uniquefloat_132", "uniquefloat_133", "uniquefloat_134", "uniquefloat_135"
            , "uniquefloat_136", "uniquefloat_137", "uniquefloat_138", "uniquefloat_139", "uniquefloat_140"
            , "uniquefloat_141", "uniquefloat_142", "uniquefloat_143", "uniquefloat_144", "uniquefloat_145"
            , "uniquefloat_146", "uniquefloat_147", "uniquefloat_148", "uniquefloat_149", "uniquefloat_150"
            , "uniquefloat_151", "uniquefloat_152", "uniquefloat_153", "uniquefloat_154", "uniquefloat_155"
            , "uniquefloat_156", "uniquefloat_157", "uniquefloat_158", "uniquefloat_159", "uniquefloat_160"
            , "uniquefloat_161", "uniquefloat_162", "uniquefloat_163", "uniquefloat_164", "uniquefloat_165"
            , "uniquefloat_166", "uniquefloat_167", "uniquefloat_168", "uniquefloat_169", "uniquefloat_170"
            , "uniquefloat_171", "uniquefloat_172", "uniquefloat_173", "uniquefloat_174", "uniquefloat_175"
            , "uniquefloat_176", "uniquefloat_177", "uniquefloat_178", "uniquefloat_179", "uniquefloat_180"
            , "uniquefloat_181", "uniquefloat_182", "uniquefloat_183", "uniquefloat_184", "uniquefloat_185"
            , "uniquefloat_186", "uniquefloat_187", "uniquefloat_188", "uniquefloat_189", "uniquefloat_190"
            , "uniquefloat_191", "uniquefloat_192", "uniquefloat_193", "uniquefloat_194", "uniquefloat_195"
            , "uniquefloat_196", "uniquefloat_197", "uniquefloat_198", "uniquefloat_199", "uniquefloat_200"
            , "uniquejson_001"]
        for column in columns:
            if column not in usedataDF.columns:
                usedataDF[column] = None
        df = usedataDF[columns]

        hdfsPath = "/user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/step=[:Step]/version=[:Version]/dt=[:DateNoLine]" \
            .replace("[:ProductName]", productName) \
            .replace("[:Project]", project) \
            .replace("[:Version]", version) \
            .replace("[:DateNoLine]",dt ) \
            .replace("[:Step]", step)
        hdfsFile = "[:ProductName]_[:Project]_[:Version]_[:DateNoLine]_[:Step]_[:UXTime].csv" \
            .replace("[:ProductName]", productName) \
            .replace( "[:Project]", project) \
            .replace("[:Version]", version) \
            .replace("[:DateNoLine]", dt) \
            .replace("[:Step]", step) \
            .replace("[:UXTime]", str(calendar.timegm(datetime.datetime.utcnow().utctimetuple())))
        alterDropStr = "ALTER TABLE gtwpd.model_usedata DROP IF EXISTS PARTITION (product = '[:ProductName]' , project = '[:Project]' , version = '[:Version]' , dt = '[:DateNoLine]' , step = '[:Step]'  ) " \
            .replace("[:ProductName]", productName) \
            .replace("[:Project]", project) \
            .replace("[:Version]", version) \
            .replace("[:DateNoLine]", dt) \
            .replace("[:Step]", step)
        alterAddStr = "ALTER TABLE gtwpd.model_usedata ADD PARTITION (product = '[:ProductName]' , project = '[:Project]' , version = '[:Version]' , dt = '[:DateNoLine]' , step = '[:Step]'  ) location '[:HDFSPath]'" \
            .replace("[:ProductName]", productName) \
            .replace("[:Project]", project) \
            .replace("[:Version]", version) \
            .replace("[:DateNoLine]", dt) \
            .replace("[:HDFSPath]", hdfsPath) \
            .replace("[:Step]", step)

        df.to_csv("{}/{}/file/HDFSUploadTempFile/{}".format(productName,project,hdfsFile), sep="\t", index=None, header=None)
        if overwrite == True :
            self.hdfsCtrl.deleteDirs(hdfsPath)
        self.hdfsCtrl.upload(hdfsPath + "/" + hdfsFile, "{}/{}/file/HDFSUploadTempFile/{}".format(productName,project,hdfsFile))
        self.hiveCtrl.executeSQL_TCByCount(alterDropStr, 2)
        self.hiveCtrl.executeSQL_TCByCount(alterAddStr, 2)
        time.sleep(1)
