from hdfs import InsecureClient
import pandas as pandas
import os
import gc
import time
from datetime import datetime
from functools import wraps

#時間裝飾器，計算函式執行
def timethis(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        print(func.__name__, end-start)
        return result
    return wrapper

class HDFSTool:
    #初始化
    def __init__(self):
        pass

    @timethis
    def transFileByOSReadlines(self, oriFilePathName, newFilePathName, oriEncoding=None, newEncoding=None):
        with open(oriFilePathName, "r", encoding=oriEncoding) as fin:
            data = fin.readlines()
        with open(newFilePathName, "w", encoding=newEncoding) as fout:
            fout.writelines(data)
        del data
        gc.collect()

    @timethis
    def transFileByOSSplitlines(self, oriFilePathName , newFilePathName , oriEncoding=None , newEncoding=None):
        with open(oriFilePathName, "r", encoding=oriEncoding) as fin:
            data = fin.read().splitlines(True)
        with open(newFilePathName, "w", encoding=newEncoding) as fout:
            fout.writelines(data)
        del data
        gc.collect()

    @timethis
    def transCSVByPandas(self, oriFilePathName , newFilePathName):
        dataFrames = pandas.read_csv(oriFilePathName, header=None, error_bad_lines=False)
        dataFrames.to_csv(newFilePathName, encoding='utf8', header=0, index=0)
        del dataFrames
        gc.collect()

    @timethis
    def csvCuttingByPandasToSize(self, oriFilePathName, newFilePath , newFileSize):
        oriDataFrames = pandas.read_csv(oriFilePathName, encoding ='utf-8', header=0, index=0)
        oriDataSize = os.path.getsize(oriDataFrames)
        oriDataRowCount = len(oriDataFrames)
        oriOneRowSize = oriDataSize // oriDataRowCount
        newDataRowCount = newFileSize // oriOneRowSize + 1
        makeRowCount = 0
        while makeRowCount < oriDataRowCount:
            minOriRowCount = makeRowCount
            maxOriRowCount = makeRowCount + newDataRowCount - 1 if makeRowCount + newDataRowCount <= oriDataRowCount else oriDataRowCount
            newFilePathName = newFilePath + "/FlumeData." + str(round(time.time() * 1000))
            newDataFrames = oriDataFrames[minOriRowCount:maxOriRowCount]
            newDataFrames.to_csv(newFilePathName, encoding='utf-8', header=0, index=0)
            del newDataFrames
            gc.collect()
            makeRowCount = makeRowCount + newDataRowCount
            print("Info:File " + newFilePathName + " is save!")
        del oriDataFrames
        gc.collect()

    @timethis
    def csvCuttingByPandasToCount(self, oriFilePathName, newFilePath ,newFileCount):
        oriDataFrames = pandas.read_csv(oriFilePathName, encoding='utf-8', header=0, index=0)
        oriDataRowCount = len(oriDataFrames)
        newDataRowCount = oriDataRowCount // newFileCount + 1
        makeRowCount = 0
        while makeRowCount < oriDataRowCount:
            minOriRowCount = makeRowCount
            maxOriRowCount = makeRowCount + newDataRowCount - 1 if makeRowCount + newDataRowCount <= oriDataRowCount else oriDataRowCount
            newFilePathName = newFilePath + "/FlumeData." + str(round(time.time() * 1000))
            newDataFrames = oriDataFrames[minOriRowCount:maxOriRowCount]
            newDataFrames.to_csv(newFilePathName, encoding='utf-8', header=0, index=0)
            del newDataFrames
            gc.collect()
            makeRowCount = makeRowCount + newDataRowCount
            print("Info:File " + newFilePathName + " is save!")
        del oriDataFrames
        gc.collect()

    @timethis
    def csvCuttingByOSToSize(self, oriFilePathName, newFilePath, newFileSize):
        oriData = open(oriFilePathName, "r", encoding="utf-8").readlines()
        oriDataSize = os.path.getsize(oriData)
        oriDataRowCount = len(oriData)
        oriOneRowSize = oriDataSize // oriDataRowCount
        newDataRowCount = newFileSize // oriOneRowSize + 1
        makeRowCount = 0
        while makeRowCount < oriDataRowCount:
            minOriRowCount = makeRowCount
            maxOriRowCount = makeRowCount + newDataRowCount - 1 if makeRowCount + newDataRowCount <= oriDataRowCount else oriDataRowCount
            newFilePathName = newFilePath + "/FlumeData." + str(round(time.time() * 1000))
            newDataFrames = oriData[minOriRowCount:maxOriRowCount]
            newDataFrames.to_csv(newFilePathName, encoding='utf-8', header=0, index=0)
            del newDataFrames
            gc.collect()
            makeRowCount = makeRowCount + newDataRowCount
            print("Info:File " + newFilePathName + " is save!")
        del oriData
        gc.collect()

    @timethis
    def csvCuttingByOSToCount(self, oriFilePathName, newFilePath, newFileCount):
        oriData = open(oriFilePathName, "r", encoding="utf-8").readlines()
        oriDataRowCount = len(oriData)
        newDataRowCount = oriDataRowCount // newFileCount + 1
        makeRowCount = 0
        while makeRowCount < oriDataRowCount:
            minOriRowCount = makeRowCount
            maxOriRowCount = makeRowCount + newDataRowCount - 1 if makeRowCount + newDataRowCount <= oriDataRowCount else oriDataRowCount
            newFilePathName = newFilePath + "/FlumeData." + str(round(time.time() * 1000))
            newDataFrames = oriData[minOriRowCount:maxOriRowCount]
            newDataFrames.to_csv(newFilePathName, encoding='utf-8', header=0, index=0)
            del newDataFrames
            gc.collect()
            makeRowCount = makeRowCount + newDataRowCount
            print("Info:File " + newFilePathName + " is save!")
        del oriData
        gc.collect()

    @timethis
    def removeDirAllFile(self, oriFilePath):
        pathNewFileNames = os.listdir(oriFilePath)
        for deleteFileName in pathNewFileNames:
            deleteFileName = oriFilePath + "/" + deleteFileName
            os.remove(deleteFileName)
