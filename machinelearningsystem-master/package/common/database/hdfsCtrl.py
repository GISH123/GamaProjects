from hdfs import InsecureClient
import pandas as pd
import time

class HDFSCtrl:
    #初始化
    def __init__(self, url=None, user=None, password=None, filePath=None):
        self.__url = url
        self.__user = user
        self.__password = password
        self.__filePath = filePath

    # 列出hdfs_path目錄下的檔案列表
    def list(self, hdfs_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user,root=self.__filePath)
        return hdfs_client.list(hdfs_path)

    # 建立hdfs_path目錄
    def makeDirs(self, hdfs_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user,root=self.__filePath)
        hdfs_client.makedirs(hdfs_path)

    # 刪除hdfs_path目錄
    def deleteDirs(self, hdfs_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user, root=self.__filePath)
        hdfs_client.delete(hdfs_path,recursive=True, skip_trash=True)

    # 上傳本地的local_path檔案至Hadoop的hdfs_path
    def upload(self, hdfs_path,local_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user,root=self.__filePath)
        try:
            hdfs_client.upload(hdfs_path, local_path, overwrite=True, cleanup=True)
        except:
            self.makeDirs('/'.join(hdfs_path.split("/")[:-1]))
            time.sleep(1)
            hdfs_client.upload(hdfs_path, local_path, overwrite=True, cleanup=True)

    # 下載Hadoop的hdfs_path至本地的local_path檔案
    def download(self, hdfs_path, local_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user,root=self.__filePath)
        hdfs_client.download(hdfs_path, local_path, overwrite=True)

    # 將hdfs_path的CSV檔案直接解析成DataFrame
    def loadCSVToPandas(self, hdfs_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user,root=self.__filePath)
        with hdfs_client.read(hdfs_path, encoding="utf-8") as reader:
            df = pd.read_csv(reader,index_col=0)
            return df

    # 將hdfs_path的檔案直接解析成字串
    def loadToStr(self, hdfs_path):
        hdfs_client = InsecureClient(self.__url, user=self.__user,root=self.__filePath)
        with hdfs_client.read(hdfs_path, encoding="utf-8") as reader:
            data = reader.read()
            s = str(data)
            return s

