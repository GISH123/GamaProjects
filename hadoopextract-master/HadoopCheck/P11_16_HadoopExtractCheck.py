import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/Telegram.env")
load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")

sqlTool = SqlTool()
extracthCtrl = ExtracthCtrl()

sshCtrl_hdfs = SSHCtrl(
    host="10.10.99.131"
    , port=22
    , usr="hdfs"
    , pkey="env/ALL_PKEY_HDFS"
)

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

greenplumCtrl = PostgresCtrl(
    host="10.10.99.151"
    , port=int(os.getenv("POSTGRES_PORT"))
    , user="gpadmin"
    , password="!QAZ2wsx"
    , database="hadoopcheck"
    , schema="public"
)

telegramCtrl = TelegramCtrl()

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameNameArr = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameNameArr = parametersData[key]


    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=30)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameNameArr = ["bf","maple","tdn","els","mabi","lineage","cso","bnb","kr"] if gameNameArr == [] else gameNameArr

    makeTimeArr = []
    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else:
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeTime in makeTimeArr:
        print("Run [HadoopExtractCheck] to {}".format(makeTime.strftime("%Y-%m-%d")))
        #getHadoopData(makeTime, gameNameArr)
        #checkHadoopData(makeTime, gameNameArr)

    sendMessage(makeTime)
    sendMessageBefore14(makeTime,gameNameArr)

"""
    05-02-01 根據參數撈取Hadoop資料並塞資料庫

    1. 刪除DB舊資料
    2. 使用指令hadoop dfs -du /user/hive/warehouse/{}.db/ALL/{}/dt={} 撈取相關遊戲資料
    3. 塞入相關檔案資料
"""
def getHadoopData(makeTime, gameNameArr):
    # 清除當天DB資料
    # ====================================================================================================

    for gameName in gameNameArr :
        deleteSqlStr = """
           DELETE FROM hadoopextract.datasize
           WHERE 1 = 1
               AND datatime >= '[:StartDate]' 
               AND datatime < '[:EndDate]'
               AND ('Game' = '[:GameName]' OR gamename = '[:GameName]')
        """
        deleteSqlStr = deleteSqlStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
        deleteSqlStr = deleteSqlStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        deleteSqlStr = deleteSqlStr.replace("[:GameName]", gameName)
        greenplumCtrl.executeSQL(deleteSqlStr)

        # 新增當天DB資料
        # ====================================================================================================
        print("撈取" + makeTime.strftime("%Y%m%d") + "當天資料")

        hadoopDataDF = getHadoopDataDF(makeTime,gameName)

        if hadoopDataDF.size == 0:
            hadoopDataDF = getHadoopDataDF(makeTime,gameName)

        if hadoopDataDF.size == 0:
            hadoopDataDF = getHadoopDataDF(makeTime,gameName)

        tableName = "hadoopextract.datasize"
        tableInfoDF = greenplumCtrl.getTableInfo(tableName)
        greenplumCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=hadoopDataDF)

def checkHadoopData(makeTime, gameNameArr):
    for gameName in gameNameArr :
        sqlReplaceArr = [
            ['[:SelectDate]', makeTime.strftime("%Y-%m-%d")]
            , ['[:GameName]', gameName]
        ]

        sqlStrArr = SqlTool().loadfile("HadoopCheck/P11_16_HadoopExtractCheck.sql").makeSQLStrs(sqlReplaceArr).makeSQLArr().getSQLArr()

        for sqlStr in sqlStrArr:
            greenplumCtrl.executeSQL(sqlStr)

def sendMessage(makeTime) :
    selectSqlSizeStr = """
        SELECT 
            sum(datasize) as datasize
        FROM hadoopextract.datasize AAA
        WHERE 1 = 1 
            AND AAA.datatime >= '[:StartDate]' 
            AND AAA.datatime < '[:EndDate]'
    """
    selectSqlSizeStr = selectSqlSizeStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
    selectSqlSizeStr = selectSqlSizeStr.replace("[:EndDate]",(makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))

    selectSqlTotalStr = """
        SELECT 
           AAA.datatime
           , AAA.gamename
           , SUM(errorcount) as errortablecount
           , SUM(errorsize) as errortablesize
        FROM hadoopextract.datacheck AAA
        WHERE 1 = 1 
            AND AAA.datatime >= '[:StartDate]' 
            AND AAA.datatime < '[:EndDate]'
        GROUP BY
           AAA.datatime
           , AAA.gamename
        ORDER BY
            AAA.datatime DESC
    """
    selectSqlTotalStr = selectSqlTotalStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
    selectSqlTotalStr = selectSqlTotalStr.replace("[:EndDate]",(makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))

    selectSqlDetailStr = """
        SELECT 
           AAA.*
        FROM hadoopextract.datacheckdetail AAA
        WHERE 1 = 1 
            AND AAA.datatime >= '[:StartDate]' 
            AND AAA.datatime < '[:EndDate]'
        ORDER BY
            AAA.datatime DESC
            , AAA.errorsize DESC
        limit 20
    """
    selectSqlDetailStr = selectSqlDetailStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
    selectSqlDetailStr = selectSqlDetailStr.replace("[:EndDate]",(makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))

    checkSizeDF = greenplumCtrl.searchSQL(selectSqlSizeStr)
    checkTotalDF = greenplumCtrl.searchSQL(selectSqlTotalStr)
    checkDetailDF = greenplumCtrl.searchSQL(selectSqlDetailStr)

    massage = "{} 大數據中繼站檢查 \n".format(makeTime.strftime("%Y-%m-%d"))
    if len(checkTotalDF) == 0 and len(checkSizeDF) != 0:
        massage += "中繼站製作正常 \n"
        for index, row in checkSizeDF.iterrows():
            massage += "匯入資料量： {} \n".format(sizeToStr(row["datasize"]))
    elif len(checkTotalDF) == 0 and len(checkSizeDF) == 0:
        massage += "checkSize 為 0 請檢查相關資料 \n"
    else:
        massage += "未完整製作遊戲數量 : {} \n".format(str(len(checkTotalDF)))

    if len(checkTotalDF) != 0:
        massage += "============================= \n"
        massage += "遊戲名稱 錯誤數量 錯誤容量\n".format(str(len(checkTotalDF)))

    for index, row in checkTotalDF.iterrows():
        massage += "{} {} {} \n".format(row["gamename"], row["errortablecount"], sizeToStr(row["errortablesize"]))

    if len(checkDetailDF) != 0:
        massage += "============================= \n"
        massage += "遊戲名稱 DB Table 錯誤數量 錯誤容量\n".format(str(len(checkTotalDF)))
    print(checkDetailDF)

    for index, row in checkDetailDF.iterrows():
        massage += "{} | {} | {} | {} | {} \n".format(row["gamename"], row["dbname"], row["tablename"], row["errorcount"],sizeToStr(row["errorsize"]))

    if len(checkDetailDF) >= 20:
        massage += "等等其他項資料　\n".format(str(len(checkTotalDF)))

    print(massage)
    telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"),massage=massage)

def sendMessageBefore14(makeTime,gameNameArr) :
    selectSqlTotalStr = """
        SELECT  
           AAA.datatime
           , AAA.gamename
           , SUM(errorcount) as errortablecount
           , SUM(errorsize) as errortablesize
        FROM hadoopextract.datacheck AAA
        WHERE 1 = 1 
            AND AAA.datatime >= '[:StartDate]' 
            AND AAA.datatime < '[:EndDate]'
            AND AAA.gamename = '[:GameName]'
        GROUP BY
           AAA.gamename
           , AAA.datatime
        ORDER BY
           AAA.gamename
           , AAA.datatime DESC
    """
    selectSqlTotalStr = selectSqlTotalStr.replace("[:StartDate]",(makeTime - datetime.timedelta(days=15)).strftime("%Y-%m-%d"))
    selectSqlTotalStr = selectSqlTotalStr.replace("[:EndDate]",(makeTime + datetime.timedelta(days=0)).strftime("%Y-%m-%d"))

    for gameName in gameNameArr :
        selectSqlGameStr = selectSqlTotalStr.replace("[:GameName]", gameName)
        checkTotalDF = greenplumCtrl.searchSQL(selectSqlGameStr)

        if len(checkTotalDF) != 0:
            massage = "{} 前兩周中繼站製作錯誤檢查 \n".format(makeTime.strftime("%Y-%m-%d"))
            massage += "============================= \n"
            massage += "錯誤日期 遊戲名稱 錯誤表數 錯誤容量\n".format(str(len(checkTotalDF)))
            for index, row in checkTotalDF.iterrows():
                massage += "{} | {} | {} | {} \n".format(row["datatime"].strftime("%Y-%m-%d"), row["gamename"],
                                                   row["errortablecount"], sizeToStr(row["errortablesize"]))

            print(massage)
            telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"),massage=massage)

#------------------------------------------------------------------------------------------

def getHadoopDataDF(makeTime,gameName):

    shReplaceArr = [
        ["[:GameName]", gameName]
        , ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
        , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
    ]

    for shHadoopStr in getHadoopSHStrArr(gameName) :
        for shReplace in shReplaceArr :
            shHadoopStr = shHadoopStr.replace(shReplace[0],shReplace[1])
        print(shHadoopStr)
        execRetrunStr = sshCtrl_hdfs.ssh_exec_cmd_return(shHadoopStr)
        retrunStrArr = execRetrunStr.split("\r\n")
        mainDataList = []
        try :
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
                    mainData = makeMainData(makeTime, gameName, pathSizeData, pathData)
                    mainDataList.append(mainData)
        except Exception as e:
            print(e)
            pass
        mainDF = pandas.DataFrame(mainDataList, columns=["datatime", "gamename", "dbname", "world", "tablename", "datasize"])
        mainDFgroup = mainDF.groupby(by=["datatime", "gamename", "dbname", "world", "tablename"], as_index=False)[["datasize"]].sum()
        print(mainDFgroup)
        return mainDFgroup

def makeMainData(makeTime,gameName,pathSizeData,pathData) :
    mainData = [None,None,None,None,None,None]
    if gameName in ["bf","tdn","els","mabi","lineage","cso","bnb","kr"] :
        if pathData[7].find("world=") >= 0:
            mainData = [makeTime, pathData[3].replace("_extract.db", ""), pathData[4], pathData[7].replace("world=", ""), pathData[5], int(pathSizeData[0])]
        else:
            mainData = [makeTime, pathData[3].replace("_extract.db", ""), pathData[4], "COMMON", pathData[5], int(pathSizeData[0])]
    elif gameName in ["maple"] :
        if pathData[6].find("world=") >= 0:
            mainData = [makeTime, pathData[3].replace("_extract.db", ""), "COMMON", pathData[6].replace("world=", ""), pathData[4], int(pathSizeData[0])]
        else:
            mainData = [makeTime, pathData[3].replace("_extract.db", ""), "COMMON", "COMMON", pathData[4], int(pathSizeData[0])]
    return mainData

def getHadoopSHStrArr (gameName) :
    if gameName in ["bf", "tdn", "els", "mabi", "lineage", "cso", "bnb", "kr"]:
        return ["hdfs dfs -du -s /user/hive/warehouse/[:GameName]_extract.db/[!a]*/*/dt=[:DateNoLine]/[!w]*/"
                , "hdfs dfs -du -s /user/hive/warehouse/[:GameName]_extract.db/[!a]*/*/dt=[:DateNoLine]/world=*/*"]
    elif gameName in ["maple"]:
        return ["hdfs dfs -du -s /user/hive/warehouse/maple_extract.db/[!a]*/dt=[:DateNoLine]/[!w]*/"
                ,"hdfs dfs -du -s /user/hive/warehouse/[:GameName]_extract.db/[!a]*/dt=[:DateNoLine]/world=*/*"]

def sizeToStr(filesize) :
    if filesize >= 1000000000000 :
        return str(round(filesize / 1000000000000,3)) + "T"
    elif filesize >= 1000000000 :
        return str(round(filesize / 1000000000,2)) + "G"
    elif filesize >= 1000000 :
        return str(round(filesize / 1000000, 2)) + "M"
    elif filesize >= 1000 :
        return str(round(filesize / 1000, 2)) + "K"
    else :
        return str(round(filesize, 2)) + "B"

#------------------------------------------------------------------------------------------

if __name__ == "__main__":
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    main({"startdate": [startDateStr], "enddate": [endDateStr]})

