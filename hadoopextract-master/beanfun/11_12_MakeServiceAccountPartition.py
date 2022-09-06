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


def Main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    '''startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    # 資料匯入的開始時間 2020-05-07開始有資料
    # startDateStr = "2020-05-26"
    # 資料匯入的結束時間
    # endDateStr = "2020-05-29"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:'''

    makeTimeArr = [
        nowZeroTime - datetime.timedelta(days=5)
        , nowZeroTime - datetime.timedelta(days=2)
        , nowZeroTime - datetime.timedelta(days=1)
    ]
    for makeTime in makeTimeArr:
        hiveSingleCtrl.startConnect()
        sqlQueryStr = "SELECT servicecode FROM bf_all.beanfundb_d_serviceaccount_f " \
                      "WHERE dt >= DATE_FORMAT(DATE_ADD('" + makeTime.strftime("%Y-%m-%d") + "', -3), 'yyyyMMdd') " \
                      "AND   dt <= DATE_FORMAT(DATE_ADD('" + makeTime.strftime("%Y-%m-%d") + "', 3), 'yyyyMMdd') " \
                      "GROUP BY servicecode"
        # print(sqlQueryStr)
        bfServiceCode = hiveSingleCtrl.searchSQL_TCByCount(sqlQueryStr, 3)

        # 建立Table
        try:
            # 確認Table是否存在
            hiveSingleCtrl.executeSQL_TCByCount('show columns in bf_extract.p_serviceaccount', 3)
        except:
            createStr = "CREATE External TABLE IF NOT EXISTS bf_extract.p_serviceaccount ( \n" \
                            + "source string ,mainaccount string ,serviceaccountid string ,createtime string ,serviceregion string \n) " \
                            + "PARTITIONED BY ( dt string , servicecode string ) \n" \
                            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' \n" \
                            + "LOCATION '/user/hive/warehouse/bf.db/ALL/Table'  \n"
            # 建立Table
            # print(createStr)
            hiveSingleCtrl.executeSQL_TCByCount(createStr, 3)

        startRunTime = time.time()
        InsertDailyData(makeTime, bfServiceCode)
        MakePartitionDetail(makeTime, bfServiceCode)
        print("Insert", makeTime.strftime("%Y-%m-%d"), "Data Total Used", time.time() - startRunTime, "seconds.")

        # 確認資料是否有正常寫入
        if hiveCtrl.searchSQL_TCByCount("select * from bf_extract.p_serviceaccount where dt = '" + makeTime.strftime("%Y%m%d") + "' limit 10", 3).count()['p_serviceaccount.serviceaccountid'] == 0:
            message = u'\U0000203C' + " Info: bf_extract.p_serviceaccount insert data fail."
            print(message)
            # 送出Telegrame告警
            try:
                telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
            except Exception as e:
                print(e)
                pass
        else:
            print("Info: bf_extract.p_serviceaccount insert data success.")
        makeTime = makeTime + datetime.timedelta(days=1)
        hiveSingleCtrl.closeConnect()

    print("Insert Data Total Used ", time.time() - start_time, "seconds.")


# 匯入Daily Data
def InsertDailyData(makeTime, bfServiceCode):
    insertDataSQLInitCoed1 = "-- bf MakeServiceAccountPartition by ServiceCode partition \n" \
                             + "FROM ( \n" \
                             + "SELECT DISTINCT source,mainaccount,serviceaccountid,createtime,serviceregion,servicecode \n" \
                             + "FROM bf_all.beanfundb_d_serviceaccount_f \n" \
                             + "WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd') \n" \
                             + "AND   dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd') ) tmp \n"
    insertDataSQLInitCoed2 = "INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/bf_extract.db/all/p_serviceaccount/dt=[:DateNoLine]/servicecode=[:ServiceCode]' \n" \
                             + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \n" \
                             + "SELECT source,mainaccount,serviceaccountid,createtime,serviceregion \n" \
                             + "WHERE servicecode = '[:ServiceCode]' \n"
    # 建立Insert語法
    insertDataSQL = insertDataSQLInitCoed1.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
    for row in bfServiceCode['servicecode']:
        insertDataSQL += insertDataSQLInitCoed2.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d"))
        insertDataSQL = insertDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
        insertDataSQL = insertDataSQL.replace("[:ServiceCode]", row)
    # 查看匯入語法
    # print(insertDataSQL)
    # 執行匯入語法
    hiveSingleCtrl.executeSQL_TCByCount(insertDataSQL, 3)


# 建立Partition
def MakePartitionDetail(makeTime, bfServiceCode):
    dropPartitionCodeInit = "ALTER TABLE bf_extract.p_serviceaccount DROP IF EXISTS PARTITION ( dt='[:DateNoLine]', servicecode='[:ServiceCode]' ) ;"
    alterPartitionCodeInit = "ALTER TABLE bf_extract.p_serviceaccount ADD IF NOT EXISTS PARTITION ( dt='[:DateNoLine]', servicecode='[:ServiceCode]' ) \n" \
                             + "LOCATION '/user/hive/warehouse/bf_extract.db/all/p_serviceaccount/dt=[:DateNoLine]/servicecode=[:ServiceCode]';"

    taskDropSQLStrArr = []
    taskAlterSQLStrArr = []
    # 建立Partition語法
    for row in bfServiceCode['servicecode']:
        dropPartitionCode = dropPartitionCodeInit.replace("[:DateNoLine]",  makeTime.strftime("%Y%m%d")).replace("[:ServiceCode]", row)
        alterPartitionCode = alterPartitionCodeInit.replace("[:DateNoLine]",  makeTime.strftime("%Y%m%d")).replace("[:ServiceCode]", row)
        taskDropSQLStrArr.append(dropPartitionCode)
        taskAlterSQLStrArr.append(alterPartitionCode)

    extracthCtrl.runsql(hiveCtrl, taskDropSQLStrArr)
    extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr)


if __name__ == "__main__":
    print('start 11_12_MakeServiceAccountPartition')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    Main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))





