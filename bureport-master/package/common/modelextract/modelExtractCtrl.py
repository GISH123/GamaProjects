import os
from package.common.database.sqlTool import SqlTool
import package.common.common.GamaniaDateTime as gamaniaDateTime
import datetime

sqlTool = SqlTool()

class ModelExtractCtrl:

    def makeOtherDetil(self, makeInfo):
        gameName = makeInfo["gameName"]
        reportName = makeInfo["reportName"]
        tableNumberArr = makeInfo["tableNumberArr"] if "tableNumberArr" in makeInfo.keys() else []
        layerArr = makeInfo["layerArr"] if "layerArr" in makeInfo.keys() else []
        layerInfoArrMap = makeInfo["layerInfoArrMap"] if "layerInfoArrMap" in makeInfo.keys() else []
        eval(f"exec('from sql.{gameName}.{reportName}.ReportMain import ReportMain')")
        reportMain = eval("ReportMain()")
        layerTaskSQLStrArrMap = {}
        for layer in layerArr :
            taskSQLStrArr = []
            for tableNumber in layerInfoArrMap[layer] :
                if tableNumber in tableNumberArr :
                    reportType, reportSQLArray = eval(f"reportMain.insert{tableNumber}DataSQL({makeInfo})")

                    if reportType == "OrderInsert" :
                        reportSQLArr =  self.__makeOrderInsertSQL(makeInfo,reportSQLArray)
                    elif reportType == "MutiInsert" :
                        reportSQLArr = self.__makeMutiInsertSQL(makeInfo,reportSQLArray)

                    for reportSQL in reportSQLArr :
                        taskSQLStrArr.append(reportSQL)
            layerTaskSQLStrArrMap[layer] = taskSQLStrArr
        return layerTaskSQLStrArrMap

    def __makeOrderInsertSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "tableNumberArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []
        for reportSQL in reportSQLArray:
            for sqlReplace in sqlReplaceArr:
                reportSQL = reportSQL.replace(sqlReplace[0], sqlReplace[1])
            if reportSQL.find("[:World]") >= 0:
                for worldName in worldNameArr:
                    reportSQL_world = reportSQL.replace("[:World]", worldName)
                    taskSQLStrArr.append(reportSQL_world)
            else:
                taskSQLStrArr.append(reportSQL)
        return taskSQLStrArr

    def __makeMutiInsertSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "tableNumberArr" in makeInfo.keys() else []
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
