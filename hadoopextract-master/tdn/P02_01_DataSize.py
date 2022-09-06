import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.width",2000)

load_dotenv(dotenv_path="env/PostgreSQL.env")

sshCtrl_hdfs = SSHCtrl(
    host="10.10.99.131"
    , port=22
    , usr="hdfs"
    , pkey="env/ALL_PKEY_HDFS"
)

postgresCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST")
    , port=int(os.getenv("POSTGRES_PORT"))
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="Hadoop"
    , schema="public"
)

"""
    05-02 撈取Hadoop資料並回塞資料庫

    1. 確認相關參數 startdate enddate gamename dbname
    2. 執行 05-02-01 根據參數撈取Hadoop資料並塞資料庫
"""

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dbName = ""

    if parametersData == {} :
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys() :
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "dbname":
            dbName = parametersData[key][0]

    if makeDateStrArr == [] :
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "tdn" if gameName == "" else gameName
    dbName = "DB" if dbName == "" else dbName

    makeTimeArr = []
    if makeDateStrArr != [] :
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else :
        startDateTime = datetime.datetime.strptime(startDateStr,"%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr,"%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeTime in makeTimeArr:
        print("Run [MakeDataSize] to {}".format(makeTime.strftime("%Y-%m-%d")))
        getHadoopData(makeTime, gameName, dbName)

"""
    05-02-01 根據參數撈取Hadoop資料並塞資料庫

    1. 刪除DB舊資料
    2. 使用指令hadoop dfs -du /user/hive/warehouse/{}.db/ALL/{}/dt={} 撈取相關遊戲資料
    3. 塞入相關檔案資料
"""
def getHadoopData (makeZeroTime,gameName,dbName):

    # 清除當天DB資料
    # ====================================================================================================

    deleteSqlStr = """
               DELETE FROM datacheck.datasize
               WHERE 1 = 1
                   AND datatime >= '[:StartDate]' 
                   AND datatime < '[:EndDate]'
                   AND ('Game' = '[:GameName]' OR gamename = '[:GameName]')
                   AND ('DB' = '[:DBName]' OR dbname = '[:DBName]')
           """
    deleteSqlStr = deleteSqlStr.replace("[:StartDate]", makeZeroTime.strftime("%Y-%m-%d"))
    deleteSqlStr = deleteSqlStr.replace("[:EndDate]", (makeZeroTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    deleteSqlStr = deleteSqlStr.replace("[:GameName]", gameName)
    deleteSqlStr = deleteSqlStr.replace("[:DBName]", dbName)
    postgresCtrl.executeSQL(deleteSqlStr)

    # 新增當天DB資料
    # ====================================================================================================
    print("撈取" + makeZeroTime.strftime("%Y%m%d") + "當天資料")


    hadoopDataDF = getHadoopDataDF(makeZeroTime,gameName,dbName)


    if hadoopDataDF.size == 0 :
        hadoopDataDF = getHadoopDataDF(makeZeroTime, gameName, dbName)

    if hadoopDataDF.size == 0 :
        hadoopDataDF = getHadoopDataDF(makeZeroTime, gameName, dbName)

    tableName = "datacheck.datasize"
    tableInfoDF = postgresCtrl.getTableInfo(tableName)
    postgresCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=hadoopDataDF)



def getHadoopDataDF (makeZeroTime,gameName,dbName):
    hadoopStr = "hadoop dfs -du /user/hive/warehouse/{}.db/ALL/[!Table]{}/dt={}/*".format(gameName,('*' if dbName == 'DB' else dbName),makeZeroTime.strftime("%Y%m%d"))
    #print(hadoopStr)
    execRetrunStr = sshCtrl_hdfs.ssh_exec_cmd_return(hadoopStr)
    retrunStrArr = execRetrunStr.split("\r\n")
    mainDataList = []
    for retrunStr in retrunStrArr:
        #print(retrunStr)
        if retrunStr == "DEPRECATED: Use of this script to execute hdfs command is deprecated." :
            continue
        elif retrunStr == "Instead use the hdfs command for it." :
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
            mainData = [makeZeroTime, pathData[3].replace(".db", ""), pathData[5], pathData[7], pathData[8], pathSizeData[0]]

            mainDataList.append(mainData)

    mainDF = pandas.DataFrame(mainDataList, columns=["datatime", "gamename", "dbname", "tablename" ,"filename" , "datasize"])

    # 根據資料檢查 _SUCCESS
    tableFilterArray = [
        ["filename", r"_SUCCESS", False]
        , ["filename", r"_temporary", False]
    ]

    mainDFMask1 = mainDF["filename"].str.contains(r"csv")
    mainDFMask2 = mainDF["filename"].str.contains(r"part-m-")
    mainDFMask3 = mainDF["filename"].str.contains(r"_SUCCESS")
    mainDFMask4 = mainDF["filename"].str.contains(r"_temporary")
    mainDFcheck = mainDF[~(mainDFMask1 | mainDFMask2 | mainDFMask3 | mainDFMask4)]
    print(mainDFcheck)

    for tableFilter in tableFilterArray:
        mainDFMask = mainDF[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True:
            mainDF = mainDF[mainDFMask]
        else:
            mainDF = mainDF[~mainDFMask]

    mainDFgroup =  mainDF.groupby(by=["datatime", "gamename", "dbname", "tablename"],as_index=False)[["datasize"]].sum()

    #print(mainDFgroup)

    return mainDFgroup

if __name__ == "__main__" :
    main()