import os
from package.common.database.sqlTool import SqlTool
import package.common.common.GamaniaDateTime as gamaniaDateTime
import datetime
import copy

sqlTool = SqlTool()

class ModelExtractCtrl:

    def makeOtherDetil(self, makeInfo):
        gameName = makeInfo["gameName"]
        reportName = makeInfo["reportName"]
        tableNumberArr = makeInfo["tableNumberArr"] if "tableNumberArr" in makeInfo.keys() else []
        layerArr = makeInfo["layerArr"] if "layerArr" in makeInfo.keys() else []
        layerInfoArrMap = makeInfo["layerInfoArrMap"] if "layerInfoArrMap" in makeInfo.keys() else []
        tableNumberInfoMap = makeInfo["tableNumberInfoMap"] if "tableNumberInfoMap" in makeInfo.keys() else {}
        layerTaskSQLStrArrMap = {}
        for layer in layerArr :
            taskSQLStrArr = []
            for tableNumber in layerInfoArrMap[layer] :
                if tableNumber in tableNumberArr :
                    for interDependenceTableNumber in \
                            tableNumberInfoMap[tableNumber]["interDependence"] \
                            if tableNumber in tableNumberInfoMap.keys() and "interDependence" in tableNumberInfoMap[tableNumber].keys() \
                            else [] :
                        if interDependenceTableNumber not in tableNumberArr :
                            tableNumberArr.append(interDependenceTableNumber)
                    makeGameNameArr = tableNumberInfoMap[tableNumber]["interDependenceGame"] \
                            if tableNumber in tableNumberInfoMap.keys() and "interDependenceGame" in tableNumberInfoMap[tableNumber].keys() \
                            else [gameName]
                    for makeGameName in makeGameNameArr :
                        eval(f"exec('from sql.{makeGameName}.{reportName}.ReportMain import ReportMain as {makeGameName}ReportMain')")
                        reportMain = eval(f" {makeGameName}ReportMain()")
                        reportType, reportSQLArray = eval(f"reportMain.insert{tableNumber}DataSQL({makeInfo})")
                        makeCopyInfo = copy.deepcopy(makeInfo)
                        makeCopyInfo['gameName'] = makeGameName
                        makeCopyInfo["sqlReplaceArr"] = []
                        for sqlReplace in  makeInfo["sqlReplaceArr"] :
                            makeCopyInfo["sqlReplaceArr"].append(["[:GameName]", makeGameName] if sqlReplace[0] == "[:GameName]" else sqlReplace)
                        if reportType == "OrderInsert" :
                            reportSQLArr = self.__makeOrderInsertSQL(makeCopyInfo,reportSQLArray)
                        elif reportType == "MutiInsert" :
                            reportSQLArr = self.__makeMutiInsertSQL(makeCopyInfo,reportSQLArray)
                        for reportSQL in reportSQLArr :
                            taskSQLStrArr.append(reportSQL)
            layerTaskSQLStrArrMap[layer] = taskSQLStrArr
        return layerTaskSQLStrArrMap

    def __makeOrderInsertSQL(self, makeInfo, reportSQLArray):
        taskSQLStrArr = []
        worldNameArr = makeInfo["worldNameArr"] if "worldNameArr" in makeInfo.keys() else []
        gameIdCodeArr = makeInfo["gameIdCodeArr"] if "gameIdCodeArr" in makeInfo.keys() else []
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
        elif reportSQLArray[0].find("[:Game]") >= 0:
            for gameIdCode in gameIdCodeArr:
                reportSQLAll = ""
                for reportSQL in reportSQLArray:
                    for sqlReplace in sqlReplaceArr:
                        reportSQL = reportSQL.replace(sqlReplace[0], sqlReplace[1])
                    reportSQL = reportSQL.replace("[:Game]", gameIdCode[1])
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
        gameIdCodeArr = makeInfo["gameIdCodeArr"] if "gameIdCodeArr" in makeInfo.keys() else []
        sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []

        fromInitSQLCode = reportSQLArray[0]
        mutiInsertInitSQLCode = reportSQLArray[1]
        insertDataSQL = fromInitSQLCode
        if reportSQLArray[1].find("[:Game]") >= 0:
            for gameIdCode in gameIdCodeArr:
                insertDataSQL += mutiInsertInitSQLCode.replace("[:Game]", gameIdCode[1]).replace("[:ServiceCode]", gameIdCode[0])
        elif reportSQLArray[1].find("[:World]") >= 0:
            for worldName in worldNameArr:
                insertDataSQL += mutiInsertInitSQLCode.replace("[:World]", worldName)
        else:
            insertDataSQL += mutiInsertInitSQLCode

        for sqlReplace in sqlReplaceArr:
            insertDataSQL = insertDataSQL.replace(sqlReplace[0], sqlReplace[1])

        insertDataSQL = insertDataSQL + ";"
        taskSQLStrArr.append(insertDataSQL)
        return taskSQLStrArr
