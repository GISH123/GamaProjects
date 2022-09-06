import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.inputCtrl import inputCtrl
from dotenv import load_dotenv
from functools import reduce
import operator
import datetime
import time

load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(
      url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)

def main(parametersData = {}):
    try :
        hdfsCtrl.upload(parametersData["hdfsPath"],parametersData["localPath"])
    except :
        hdfsCtrl.makeDirs('/'.join(parametersData["hdfsPath"].split("/")[:-1]))
        time.sleep(1)
        hdfsCtrl.upload(parametersData["hdfsPath"],parametersData["localPath"])

"""
    上傳hdfs的小工具
    "localPath" 本地的檔案路徑
    "hdfsPath" 上傳至HDFS的檔案路徑 
    備註：有關模型的檔案請一律上傳至 /user/GTW_PD/DB/Model/File/[RawData,Preprocess,UseModel]/[:版本號] 
    裡面可以為R1_0_X 代表R1_0通用，但不可以為RX_0_10
    網頁路徑為http://bd-cm-01:8889/hue/filebrowser/view%3D/user/GTW_PD/DB/#/user/GTW_PD/DB/Model/File
    多檔案上傳就請多寫幾個main
"""
if __name__ == "__main__":
    main({
        "localPath" : "file/ctrl/maple_AuctionTag_R1_9_0_rawdataCtrl.csv"
        , "hdfsPath" : "/user/GTW_PD/DB/Model/File/RawData/R0_0_0/maple_AuctionTag_R1_9_0_rawdataCtrl.csv"
    })

