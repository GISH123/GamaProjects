import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.inputCtrl import inputCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from dotenv import load_dotenv
import pandas
import datetime
import time

load_dotenv(dotenv_path="env/hive.env")
extracthCtrl = ExtracthCtrl()

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

sshCtrl_hdfs = SSHCtrl("10.10.99.138", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

hdfsCtrl = HDFSCtrl(
    url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    worldNameArr = []
    tableNumberArr = []

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
            gameName = parametersData[key][0]
        if key == "worldNameArr":
            worldNameArr = parametersData[key]
        if key == "tableNumberArr":
            tableNumberArr = parametersData[key]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "lineageglobal" if gameName == "" else gameName
    worldNameArr = ["162", "165", "166", "167"] if worldNameArr == [] else worldNameArr
    tableNumberArr = [] if tableNumberArr == [] else tableNumberArr

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

    databaseInfo = {
        "gameName": gameName
        , "worldNameArr": worldNameArr
        , "tableNumberArr": tableNumberArr
    }

    # dropPartitionSQLInit = "ALTER TABLE rcenter.gama_bda_modelextract DROP PARTITION(dt='20200711',world='3',tablenumber='1001');"
    # dropPartitionSQLInit = "ALTER TABLE gtwpd.business_bureport DROP PARTITION(game='[:gameName]',dt='[:dateNoLine]')"
    dropPartitionSQLInit = "ALTER TABLE gtwpd.modelextract_modelextract DROP PARTITION(game='[:gameName]',dt='[:dateNoLine]',world='[:world]')"

    # /user/GTW_PD/DB/ModelExtract/modelextract/game=lineageglobal/dt=20210516/world=162/tablenumber=1131
    # /user/GTW_PD/DB/Business/bureport/game=lineageglobal/dt=20210516/world=162/tablenumber=1131
    deleteFolderPathInit = "/user/GTW_PD/DB/ModelExtract/modelextract/game=[:gameName]/dt=[:dateNoLine]/world=[:world]"


    startRunTime = time.time()

    # Delete NoneData Folder
    print(f"Start Delete {gameName} NoneDataFolder")
    for makeTime in makeTimeArr:
        dateNoLine = makeTime.strftime("%Y%m%d")
        dateLine = makeTime.strftime("%Y-%m-%d")

        for world in worldNameArr:
            deleteFolderPath = deleteFolderPathInit.replace("[:dateNoLine]", dateNoLine).replace('[:gameName]', gameName).replace('[:world]', world)
            deleteFolderCode = f"hdfs dfs -rm -r -f {deleteFolderPath}"
            # print(deleteFolderCode)
            startRunTime_2 = time.time()
            sshCtrl_hdfs.ssh_exec_cmd(deleteFolderCode)
            print(f"Delete {dateLine} NoneDataFolder Total Used {time.time() - startRunTime_2} seconds.")
            time.sleep(3)
    print(f"Delete {gameName} NoneDataFolder Total Used {time.time() - startRunTime} seconds.")

    '''# Drop Partition
    print(f"Start Drop {gameName} Partition")
    for makeTime in makeTimeArr:
        dateNoLine = makeTime.strftime("%Y%m%d")
        dateLine = makeTime.strftime("%Y-%m-%d")

        for world in worldNameArr:
            dropPartitionSQL = dropPartitionSQLInit.replace("[:dateNoLine]", dateNoLine).replace('[:gameName]', gameName).replace('[:world]', world)
            # print(dropPartitionSQL)
            startRunTime_2 = time.time()
            hiveCtrl.executeSQL_TCByCount(dropPartitionSQL, 3, printError=False)
            print(f"Drop {dateLine} Partition Total Used {time.time() - startRunTime_2} seconds.")
    print(f"Drop {gameName} Partition Total Used {time.time() - startRunTime} seconds.")'''

if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    # main({"startdate": ["2020-02-01"], "enddate": ["2021-01-13"]})
    # 2020-08-01,2020-08-03,2020-08-11
    # err main({"startdate": ["2020-08-13"], "enddate": ["2021-01-13"]})
    # main({"startdate": ["2020-09-01"], "enddate": ["2021-01-13"]})
    # main({"startdate": ["2020-08-25"], "enddate": ["2020-08-31"]})
    # main({"startdate": ["2020-08-20"], "enddate": ["2020-08-24"]})
    # main({"startdate": ["2020-08-15"], "enddate": ["2020-08-19"]})
    # main({"startdate": ["2020-08-14"], "enddate": ["2020-08-14"]})
    # main({"startdate": ["2021-02-05"], "enddate": ["2021-02-07"]})
    # main({"startdate": ["2021-02-20"], "enddate": ["2021-02-20"]})
    main({"startdate": ["2021-05-28"], "enddate": ["2021-05-28"]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

