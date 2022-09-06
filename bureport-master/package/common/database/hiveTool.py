from hdfs import InsecureClient
import pandas as pandas
import os
import gc
import time
from datetime import datetime
from functools import wraps


class HiveTool:
    # 初始化
    def __init__(self):
        pass

    # 製作CreateExternalSQL
    def makeCreateExternalSQL(self, createStr, tableName, coulnmName, partitioned, filePath):
        createStr = createStr.replace("[:tableName]", tableName)
        createStr = createStr.replace("[:partitioned]", partitioned)
        createStr = createStr.replace("[:filePath]", filePath)
        createStr = createStr.replace("[:coulnmName]", coulnmName)
        return createStr

    # 製作AlterExternalSQL
    def makeAlterExternalSQL(self, alterStr, tableName, partitioned, filePath):
        alterStr = alterStr.replace("[:tableName]", tableName)
        alterStr = alterStr.replace("[:partitioned]", partitioned)
        alterStr = alterStr.replace("[:filePath]", filePath)
        return alterStr

    def getCreateCoulumSQL(self ,fileDataFrames):
        coulnmName = ""
        for column in fileDataFrames.columns:
            dtypes = str(fileDataFrames[column].dtypes)
            if dtypes.find("int") >= 0:
                coulnmName = coulnmName + column + " Int,"
            elif dtypes.find("datetime64") >= 0:
                coulnmName = coulnmName + column + " Timestamp,"
            elif dtypes.find("object") >= 0:
                coulnmName = coulnmName + column + " String,"
        coulnmName = coulnmName[0:-1]
        return coulnmName

    def getCreateExternalSQL(self ):
        createStr = "CREATE External TABLE [:tableName] ([:coulnmName]) " \
                    + " PARTITIONED BY ( [:partitioned] ) " \
                    + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' " \
                    + " LOCATION '[:filePath]' "
        return createStr

    def getAlterExternalStr(self):
        alterStr = "ALTER TABLE [:tableName]  ADD PARTITION ( [:partitioned] ) location '[:filePath]' "
        return alterStr