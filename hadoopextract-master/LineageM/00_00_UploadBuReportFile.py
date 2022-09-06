import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from os import listdir
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.telegramCtrl import TelegramCtrl
from dotenv import load_dotenv
import shutil, time

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

hdfsCtrl = HDFSCtrl(
    url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
    # "http://10.10.99.101:50070;http://10.10.99.102:50070", "GTW_PD", "haveagoodtime", "/user/GTW_PD"
)

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST2")
    , port=int(os.getenv("HIVE_PORT2"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

telegramCtrl = TelegramCtrl()
start_time = time.time()

def main():
    filePath = "C:\\Users\\icechiou\\PycharmProjects\\hadoopextract\\LMfile"
    fileList = [f for f in listdir(filePath) if ".csv" in f]
    # print(fileList)
    for fileName in fileList:
        DateNoLine = fileName.split("_")[3].split(".")[0]
        tablenumber = fileName.split("_")[2]
        hdfsDirInitCode = "/user/GTW_PD/DB/BUSINESSUNIT/bureport/game=lineagem/dt=[:DateNoLine]/world=COMMOD/tablenumber=[:tablenumber]"
        hdfsDir = hdfsDirInitCode.replace("[:DateNoLine]", DateNoLine).replace("[:tablenumber]", tablenumber)
        localFile = f"{filePath}\\{fileName}"
        localBakFile = f"{filePath}\\BAK\\{fileName}"

        # 建立HDFS上資料夾
        hdfsCtrl.makeDirs(hdfsDir)
        # 上傳檔案到建立的HDFS資料夾
        hdfsCtrl.upload(hdfsDir, localFile)
        # 將檔案移到BAK資料夾
        shutil.move(localFile, localBakFile)

    # 建立partition
    hiveCtrl.executeSQL_TCByCount('msck repair table gtwpd.businessunit_bureport', 3)


if __name__ == "__main__":
    print('start 00_00_UploadBuReportFile')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
