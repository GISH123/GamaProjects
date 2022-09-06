import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hiveCtrl_2 import HiveSingleCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from dotenv import load_dotenv
import pandas, datetime, time, asyncio
import re, random

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST2")
    , port=int(os.getenv("HIVE_PORT2"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

hiveSingleCtrl = HiveSingleCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

telegramCtrl = TelegramCtrl()
extracthCtrl = ExtracthCtrl()

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

loop = asyncio.get_event_loop()
start_time = time.time()

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []


def Main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    '''startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    # 資料匯入的開始時間 2020-05-07開始有資料 2019-08-08
    startDateStr = "2020-08-01"
    # 資料匯入的結束時間
    endDateStr = "2020-08-07"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:'''

    makeTimeArr = [
        nowZeroTime - datetime.timedelta(days=32)
        , nowZeroTime - datetime.timedelta(days=5)
        , nowZeroTime - datetime.timedelta(days=1)
    ]
    for makeTime in makeTimeArr:
        hiveSingleCtrl.startConnect()
        startRunTime = time.time()
        sqlQueryStr = "SELECT servicecode FROM bf_all.beanfundb_history_play " \
                      + "WHERE dt >= DATE_FORMAT(DATE_ADD('" + makeTime.strftime("%Y-%m-%d") + "', -30), 'yyyyMMdd') " \
                      + "AND   dt <= DATE_FORMAT(DATE_ADD('" + makeTime.strftime("%Y-%m-%d") + "', 30), 'yyyyMMdd') " \
                      + "GROUP BY servicecode"
        # print(sqlQueryStr)
        bfServiceCode = hiveSingleCtrl.searchSQL_TCByCount(sqlQueryStr, 3)
        # print(bfServiceCode)

        try:
            # 確認Table是否存在
            # print('show columns in bf_extract.p_dailyactiveserviceaccount')
            hiveSingleCtrl.executeSQL('show columns in bf_extract.p_dailyactiveserviceaccount')
        except:
            createStr = "CREATE External TABLE bf_extract.p_dailyactiveserviceaccount ( \n" \
                        + "guid string,serverindex string,mainaccountid string \n" \
                        + ",serviceregion string,serviceaccountid string,logintime string,logouttime string \n" \
                        + ",chargerule string,chargepoints string,chargeservicepoints string \n" \
                        + ",ipaddress string,memo string,createtime string,transactionid string \n" \
                        + ",ticketid string,diffday int \n) " \
                        + "PARTITIONED BY ( dt string , servicecode string ) \n" \
                        + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" \
                        + "LOCATION '/user/hive/warehouse/bf.db/ALL/Table'  \n"
            # 建立Table
            # print(createStr)
            hiveSingleCtrl.executeSQL_TCByCount(createStr, 3)

        print("Start Insert", makeTime.strftime("%Y-%m-%d"), "DailyActiveServiceAccount Data.")
        # DAU資料寫入
        InsertDailyActiveData(makeTime, bfServiceCode)
        # 建立partition
        MakePartitionDetail(makeTime, bfServiceCode)
        print("Insert", makeTime.strftime("%Y-%m-%d"), "Data Total Used", time.time() - startRunTime, "seconds.")

        # 確認資料是否有正常寫入
        if hiveSingleCtrl.searchSQL_TCByCount('show partitions bf_extract.p_dailyactiveserviceaccount partition (dt = ' + makeTime.strftime("%Y%m%d") + ')', 3).count()['partition'] == 0:
            message = u'\U0000203C' + " Info: bf_extract.p_dailyactiveserviceaccount insert data fail."
            print(message)
            # 送出Telegrame告警
            try:
                telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
            except Exception as e:
                print(e)
                pass
        else:
            print("Info: bf_extract.p_dailyactiveserviceaccount insert", makeTime.strftime("%Y-%m-%d"), " data success.")
        makeTime = makeTime + datetime.timedelta(days=1)
        hiveSingleCtrl.closeConnect()

    print("Insert Data Total Used ", time.time() - start_time, "seconds.")


def InsertDailyActiveData(makeTime, bfServiceCode):
    insertDataSQLInitCoed1 = "-- bf DailyActiveServiceAccount by ServiceCode partition \n" \
                             + "FROM ( \n" \
                             + "SELECT *, datediff(logouttime ,logintime ) as diffday \n" \
                             + "FROM bf_all.beanfundb_history_play \n" \
                             + "WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-30),'yyyyMMdd') \n" \
                             + "AND   dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',30),'yyyyMMdd') ) tmp \n"
    insertDataSQLInitCoed2 = "INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/bf_extract.db/all/p_dailyactiveserviceaccount/dt=[:DateNoLine]/servicecode=[:ServiceCode]' \n" \
                             + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \n" \
                             + "SELECT guid,serverindex,mainaccountid \n" \
                             + ",serviceregion,serviceaccountid,logintime,logouttime \n" \
                             + ",chargerule,chargepoints,chargeservicepoints \n" \
                             + ",ipaddress,memo,createtime,transactionid \n" \
                             + ",ticketid,diffday \n " \
                             + "WHERE logouttime >= DATE_ADD('[:DateLine]',0) \n" \
                             + "AND   logintime < DATE_ADD('[:DateLine]',1) \n" \
                             + "AND   servicecode = '[:ServiceCode]' \n"

    insertDataSQL = insertDataSQLInitCoed1.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
    for row in bfServiceCode['servicecode']:
        insertDataSQL += insertDataSQLInitCoed2.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d"))
        insertDataSQL = insertDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
        insertDataSQL = insertDataSQL.replace("[:ServiceCode]", row)
    # 查看匯入語法
    # print(insertDataSQL)
    # 執行匯入語法
    # print(insertDataSQL)
    hiveSingleCtrl.executeSQL_TCByCount(insertDataSQL, 3)


def MakePartitionDetail(makeTime, bfServiceCode):
    dropPartitionCodeInit = "ALTER TABLE bf_extract.p_dailyactiveserviceaccount DROP IF EXISTS PARTITION ( dt='[:DateNoLine]', servicecode='[:ServiceCode]' ) ;"
    alterPartitionCodeInit = "ALTER TABLE bf_extract.p_dailyactiveserviceaccount ADD IF NOT EXISTS PARTITION ( dt='[:DateNoLine]', servicecode='[:ServiceCode]' ) \n" \
                             + "LOCATION '/user/hive/warehouse/bf_extract.db/all/p_dailyactiveserviceaccount/dt=[:DateNoLine]/servicecode=[:ServiceCode]';"

    taskDropSQLStrArr = []
    taskAlterSQLStrArr = []
    for row in bfServiceCode['servicecode']:
        dropPartitionCode = dropPartitionCodeInit.replace("[:DateNoLine]",  makeTime.strftime("%Y%m%d")).replace("[:ServiceCode]", row)
        alterPartitionCode = alterPartitionCodeInit.replace("[:DateNoLine]",  makeTime.strftime("%Y%m%d")).replace("[:ServiceCode]", row)
        taskDropSQLStrArr.append(dropPartitionCode)
        taskAlterSQLStrArr.append(alterPartitionCode)

    extracthCtrl.runsql(hiveCtrl, taskDropSQLStrArr)
    extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr)


if __name__ == "__main__":
    print('start 11_13_MakeDailyActiveServiceAccount')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    Main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

