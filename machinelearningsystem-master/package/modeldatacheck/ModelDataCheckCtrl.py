import os
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from dotenv import load_dotenv
from package.common.database.sqlTool import SqlTool
import package.common.common.GamaniaDateTime as gamaniaDateTime
import datetime
import copy
import pandas as pd
import numpy as np
import time
from package.modeldatacheck.entity.ModelDataCheckInfoEntity import ModelDataCheckInfoEntity

sqlTool = SqlTool()
modelDataCheckInfoEntityCtrl = ModelDataCheckInfoEntity()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 200000000)


class ModelDataCheckCtrl () :

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
        load_dotenv(dotenv_path="env/greenplus.env")
        self.greenplumCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="mlops"
            , schema="public"
        )

    def checkModelData(self, checkInfo):
        productName = checkInfo["productName"]
        project = checkInfo["project"]
        version = checkInfo["version"]
        step = checkInfo["step"]
        makeTime = checkInfo["makeTime"]

        print(f"start check {productName} {project} {step} , version is {version} , date is {makeTime}")
        eval(f"exec('from {productName}.{project}.info.{step}Info_{project} import {step}Info_{project}  as {productName}{step}Info_{project}')")
        stepInfoMain = eval(f" {productName}{step}Info_{project}()")
        tableauInfoMap, columnInfoMap = eval(f"stepInfoMain.get{step}Info_{project}_{version}({checkInfo})")
        mapColumnArr = []
        mainColumnArr = []
        for key in columnInfoMap.keys():
            columnInfo = columnInfoMap[key]
            columnInfo['name'] = key
            checkfuncArr = columnInfo['checkfunc'] if 'checkfunc' in columnInfo.keys() else []
            for functionName in checkfuncArr:
                eval(f"exec('from package.modeldatacheck.checkinfofunction.{step}CheckFunction import {step}CheckFunction')")
                checkFunction = eval(f" {step}CheckFunction()")
                mapColumn, mainColumn = eval(f"checkFunction.{step}Check_{functionName}({columnInfo})")
                mapColumnArr.append(mapColumn)
                mainColumnArr.append(mainColumn)

        checkInfo['startDateNoLine'] = datetime.datetime.strptime(checkInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        checkInfo['endDateNoLine'] = datetime.datetime.strptime(checkInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")
        checkInfo['mapColumnArr'] = mapColumnArr
        checkInfo['mainColumnArr'] = mainColumnArr

        modelDataCheckInfoEntityCtrl.deleteCheckSQL(checkInfo)
        checkSQL = self.makeCheckSQL(checkInfo)
        checkInfoDF = self.hiveCtrl.searchSQL_TCByCount(checkSQL, 3)
        modelDataCheckInfoEntityList = []
        for checkInfoIndex, checkInfo in checkInfoDF.iterrows():
            modelDataCheckInfoEntity = modelDataCheckInfoEntityCtrl.makeModelDataCheckInfoEntityByCheckInfo(checkInfo)
            modelDataCheckInfoEntityList.append(modelDataCheckInfoEntity)
        modelDataCheckInfoEntityCtrl.insertEntityList(modelDataCheckInfoEntityList)
        print(f"end check {productName} {project} {step} , version is {version} , date is {makeTime}")

    def makeCheckSQL(self, checkInfo):
        productName = checkInfo["productName"]
        project = checkInfo["project"]
        step = checkInfo["step"]
        version = checkInfo["version"]
        startDateNoLine = checkInfo["startDateNoLine"]
        endDateNoLine = checkInfo["endDateNoLine"]
        mapColumnArr = checkInfo["mapColumnArr"]
        mainColumnArr = checkInfo["mainColumnArr"]
        checkSQL = """
select 
    '[:ProductName]' AS productname
    , '[:Project]' AS project
    , '[:Step]' AS step
    , '[:Version]' AS version
    , AAAA.dt as datadate
    , concat_ws('_',split(BBBB.checkfunc,'_')[0],split(BBBB.checkfunc,'_')[1]) AS checkcolumn 
    , split(BBBB.checkfunc,'_')[2] AS checkfunc 
    , BBBB.checkvalue as checknumbervalue
    , null as checktextvalue
from (
    select 
        AAA.dt
        , map(
            'all_data_datacount', AAA.alldata_datacount
            , [:MapColumn]
        ) as image_map
    from (
        SELECT 
            AA.dt AS dt
            , SUM(1) AS alldata_datacount
            , [:MainColumn]
        FROM gtwpd.model_usedata AA
        WHERE 1 = 1
            AND AA.product = '[:ProductName]'  
            AND AA.project = '[:Project]'  
            AND AA.step = '[:Step]'  
            AND AA.version = '[:Version]'  
            AND AA.dt >= '[:StartDateNoLine]'
            AND AA.dt <= '[:EndDateNoLine]'
        GROUP BY 
            AA.dt
    ) AAA
) AAAA
lateral view 
    explode(AAAA.image_map) BBBB as checkfunc , checkvalue
"""     .replace("[:ProductName]", productName) \
        .replace("[:Project]", project) \
        .replace("[:Step]", step) \
        .replace("[:Version]", version) \
        .replace("[:StartDateNoLine]", startDateNoLine) \
        .replace("[:EndDateNoLine]", endDateNoLine) \
        .replace("[:MapColumn]", '\n                , '.join(mapColumnArr)) \
        .replace("[:MainColumn]", '\n                , '.join(mainColumnArr))

        return checkSQL

    def alarmModelData(self, checkInfo):
        productName = checkInfo["productName"]
        project = checkInfo["project"]
        version = checkInfo["version"]
        step = checkInfo["step"]
        makeTime = checkInfo["makeTime"]

        print(f"start alarm {productName} {project} {step} , version is {version} , date is {makeTime}")
        eval(f"exec('from {productName}.{project}.info.{step}Info_{project} import {step}Info_{project}  as {productName}{step}Info_{project}')")
        stepInfoMain = eval(f" {productName}{step}Info_{project}()")
        tableauInfoMap, columnInfoMap = eval(f"stepInfoMain.get{step}Info_{project}_{version}({checkInfo})")

        eval(f"exec('from package.modeldatacheck.alarmfunction.CommonAlarmFunction import CommonAlarmFunction')")
        commonAlarmFunction = eval(f" CommonAlarmFunction()")

        if "alarmFunctionInfo" in columnInfoMap.keys():
            alarmFunctionInfo = columnInfoMap["alarmFunctionInfo"]
            if "FixedValueComparison" in alarmFunctionInfo.keys():
                columnAlarmInfo = alarmFunctionInfo["FixedValueComparison"]
                for columnKey in columnAlarmInfo.keys():
                    columnFuncAlarmInfo = columnAlarmInfo[columnKey]
                    for columnFuncKey in columnFuncAlarmInfo.keys():
                        caseWhenThenConditionArr = []
                        caseWhenThenMassageArr = []
                        for columnFuncLevel in columnFuncAlarmInfo[columnFuncKey] :
                            alarmInfo = {}
                            alarmInfo['comparison'] = columnFuncLevel[1]
                            alarmInfo['value'] = columnFuncLevel[2]
                            alarmInfo['level'] = columnFuncLevel[0]
                            caseWhenThenCondition, caseWhenThenMassage = eval(f"commonAlarmFunction.CommonAlarm_FixedValueComparison({alarmInfo})")
                            caseWhenThenConditionArr.append(caseWhenThenCondition)
                            caseWhenThenMassageArr.append(caseWhenThenMassage)
                        columnFuncAlarmAllInfo = copy.deepcopy(checkInfo)
                        columnFuncAlarmAllInfo['checkColumn'] = columnKey
                        columnFuncAlarmAllInfo['checkFunc'] = columnFuncKey
                        columnFuncAlarmAllInfo['alarmFunc'] = "FixedValueComparison"
                        columnFuncAlarmAllInfo['caseWhenThenConditionArr'] = caseWhenThenConditionArr
                        columnFuncAlarmAllInfo['caseWhenThenMassageArr'] = caseWhenThenMassageArr
                        alarmDetailSQLStrs = self.makeAlarmDetailSQL(columnFuncAlarmAllInfo)
                        alarmDetailSQLStrArr = alarmDetailSQLStrs.split(";")[:-1]
                        for alarmDetailSQL in alarmDetailSQLStrArr :
                            time.sleep(0.5)
                            self.greenplumCtrl.executeSQL(alarmDetailSQL)
            else :
                pass

        alarmOverviewSQLStrs = """
DELETE FROM modeldatacheck.modeldatacheckoverview AA
WHERE 1 = 1 
	AND AA.productname = '[:ProductName]'
	AND AA.project = '[:Project]'	
	AND AA.datadate = '[:DateLine]' ; 

INSERT INTO modeldatacheck.modeldatacheckoverview
SELECT 
	now() AS createtime
	, now() AS modifytime
	, null AS deletetime
	, nextval('modeldatacheck.modeldataalarmoverview_modeldataalarmoverviewid_seq') AS modeldatacheckdetailid
	, AA.productname 
	, AA.project
	, AA.datadate
	, SUM(1) AS count 
	, SUM(CASE WHEN AA.alarmlevel = 'A' THEN 1 ELSE 0 END) AS lv_a_errorcount
	, SUM(CASE WHEN AA.alarmlevel = 'B' THEN 1 ELSE 0 END) AS lv_b_errorcount 
	, SUM(CASE WHEN AA.alarmlevel = 'C' THEN 1 ELSE 0 END) AS lv_c_errorcount
	, SUM(CASE WHEN AA.alarmlevel = 'D' THEN 1 ELSE 0 END) AS lv_d_errorcount 
	, SUM(CASE WHEN AA.alarmlevel = 'E' THEN 1 ELSE 0 END) AS lv_e_errorcount 
	, SUM(CASE WHEN AA.alarmlevel = 'F' THEN 1 ELSE 0 END) AS lv_f_errorcount
	, SUM(CASE WHEN AA.alarmlevel = 'G' THEN 1 ELSE 0 END) AS lv_g_errorcount 
	, SUM(CASE WHEN AA.alarmlevel = 'H' THEN 1 ELSE 0 END) AS lv_h_errorcount 
FROM modeldatacheck.modeldataalarmdetail AA
WHERE 1 = 1 
	AND AA.productname = '[:ProductName]'
	AND AA.project = '[:Project]'	
	AND AA.datadate = '[:DateLine]'
	AND AA.deletetime is NULL
GROUP BY 
	AA.productname 
	, AA.project
	, AA.datadate ;	
        """.replace("[:ProductName]", productName) \
            .replace("[:Project]", project) \
            .replace("[:DateLine]", makeTime)

        alarmOverviewSQLArr = alarmOverviewSQLStrs.split(";")[:-1]
        for alarmOverviewSQL in alarmOverviewSQLArr:
            time.sleep(0.5)
            self.greenplumCtrl.executeSQL(alarmOverviewSQL)

        print(f"end alarm {productName} {project} {step} , version is {version} , date is {makeTime}")

    def makeAlarmDetailSQL(self, columnFuncAlarmInfo):
        productName = columnFuncAlarmInfo["productName"]
        project = columnFuncAlarmInfo["project"]
        step = columnFuncAlarmInfo["step"]
        version = columnFuncAlarmInfo["version"]
        checkColumn = columnFuncAlarmInfo["checkColumn"]
        checkFunc = columnFuncAlarmInfo["checkFunc"]
        alarmFunc = columnFuncAlarmInfo["alarmFunc"]
        dateLine = columnFuncAlarmInfo["makeTime"]
        caseWhenThenConditionStrArr = columnFuncAlarmInfo["caseWhenThenConditionArr"]
        caseWhenThenMassageStrArr = columnFuncAlarmInfo["caseWhenThenMassageArr"]
        alarmSQL = """
UPDATE modeldatacheck.modeldataalarmdetail AA
SET deletetime = now()
WHERE 1 = 1 
	AND AA.productname = '[:ProductName]'
	AND AA.project = '[:Project]'
	AND AA.step = '[:Step]'
	AND AA.version = '[:Version]'
	AND AA.checkcolumn = '[:CheckColumn]'
	AND AA.checkfunc = '[:CheckFunc]'
	AND AA.alarmfunc = '[:AlarmFunc]'
	AND AA.datadate = '[:DateLine]'
	AND AA.deletetime is NULL ;         
 
INSERT INTO modeldatacheck.modeldataalarmdetail 
SELECT 
	now() AS createtime
	, now() AS modifytime
	, null AS deletetime
	, nextval('modeldatacheck.modeldataalarmdetail_modeldataalarmdetailid_seq') AS modeldatacheckdetailid
	, AA.productname AS productname
	, AA.project AS project
	, AA.step AS step
	, AA.version AS version
	, AA.datadate AS version
	, AA.checkcolumn AS checkcolumn
	, AA.checkfunc AS checkfunc
	, '[:AlarmFunc]' AS alarmfunc 
	, CASE 
	    [:CaseWhenThenCondition]
	    ELSE NULL
	  END AS alarmlevel	 
    , AA.checknumbervalue
	, CASE 
	    [:CaseWhenThenMassage]
	    ELSE NULL
	  END AS alarmmessage	 
FROM modeldatacheck.modeldatacheckinfo AA
WHERE 1 = 1 
    AND AA.productname = '[:ProductName]'
	AND AA.project = '[:Project]'
	AND AA.step = '[:Step]'
	AND AA.version = '[:Version]'
	AND AA.checkcolumn = '[:CheckColumn]'
	AND AA.checkfunc = '[:CheckFunc]'
	AND AA.datadate = '[:DateLine]'
	AND AA.deletetime is NULL
	AND 
	  CASE 
	    [:CaseWhenThenCondition] 
	    ELSE NULL
	  END IS NOT null ; 
""".replace("[:ProductName]", productName) \
            .replace("[:Project]", project) \
            .replace("[:Step]", step) \
            .replace("[:Version]", version) \
            .replace("[:CheckColumn]", checkColumn) \
            .replace("[:CheckFunc]", checkFunc) \
            .replace("[:AlarmFunc]", alarmFunc) \
            .replace("[:DateLine]", dateLine) \
            .replace("[:CaseWhenThenCondition]", '\n        '.join(caseWhenThenConditionStrArr)) \
            .replace("[:CaseWhenThenMassage]", '\n         '.join(caseWhenThenMassageStrArr))

        return alarmSQL

