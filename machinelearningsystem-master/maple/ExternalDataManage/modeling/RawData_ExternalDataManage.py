import os
import pandas as pd
import numpy as np
import datetime
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.common.RawPreModel import RawPreModel

class RawData_ExternalDataManage() :

    @classmethod
    def MakeRawData_ExternalDataManage_R1_0_1(self, makeInfo):
        inputPath = f"{makeInfo['parameter']['ProjectPath']}\\{makeInfo['parameter']['ExternalDataPath']}\\{makeInfo['parameter']['ExternalDataFile']}"
        outputPath = f"{makeInfo['parameter']['ProjectPath']}\\{makeInfo['rawdataVersion']}"


        # read_equ = pd.read_excel(Path)
        read_equ = pd.read_csv(inputPath)
        # print(read_equ)
        read_equ = read_equ.drop(['Unnamed: 5'], axis=1)
        read_equ[['客觀概述']] = read_equ[['客觀概述']].fillna('')
        read_equ = read_equ.fillna(0)

        read_equ['event'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y/%m/%d')
        read_equ['Date'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y-%m-%d')
        read_equ = read_equ.reset_index(drop=True)
        read_equ['Date1'] = ''
        read_equ['Date2'] = ''
        dt_list = read_equ.Date.unique()
        for _ in range(len(dt_list)):
            read_equ.loc[read_equ.Date == dt_list[_], "Date1"] = datetime.datetime.strptime(dt_list[_], '%Y-%m-%d')
            if _ == len(dt_list)-1:
                read_equ.loc[read_equ.Date == dt_list[_], "Date2"] = \
                    datetime.datetime.strptime(dt_list[_], '%Y-%m-%d') + datetime.timedelta(days=14) + datetime.timedelta(days=-1)
            else:
                read_equ.loc[read_equ.Date == dt_list[_], "Date2"] = \
                    datetime.datetime.strptime(dt_list[_ + 1], '%Y-%m-%d') + datetime.timedelta(days=-1)
        read_equ['Prob.'] = read_equ['Prob.'].str.replace('%','').astype(float)
        read_equ = read_equ.drop(['Date','主觀描述','類似檔期','類似檔期客觀概述','類似檔期主觀概述'], axis=1)
        print(read_equ.dtypes)
        print(pd.concat([read_equ.head(1), read_equ.tail(1)]))

        ################################################################################################################
        upload_df = RawPreModel.getUploadDataFrame()
        upload_df['commondata_002'] = read_equ['Part']
        upload_df['commondata_003'] = read_equ['ItemID']
        upload_df['commondata_004'] = read_equ['Name']
        upload_df['commondata_005'] = read_equ['KR Name']
        upload_df['commondata_006'] = read_equ['Date1']
        upload_df['commondata_007'] = read_equ['Date2']
        upload_df['commondata_008'] = read_equ['客觀概述']
        upload_df['uniquefloat_001'] = read_equ['Prob.']
        upload_df['uniquefloat_002'] = read_equ[' 市價']
        upload_df['uniquefloat_003'] = read_equ['大師標籤']
        upload_df['uniquefloat_004'] = read_equ['IP合作']
        upload_df['uniquefloat_005'] = read_equ['漂浮特效']
        upload_df['uniquefloat_006'] = read_equ['暗色系']
        upload_df['uniquefloat_007'] = read_equ['螢光系']
        upload_df['uniquefloat_008'] = read_equ['柔和色系']
        upload_df['uniquefloat_009'] = read_equ['可愛風格']
        upload_df['uniquefloat_010'] = read_equ['制服風格']
        upload_df['uniquefloat_011'] = read_equ['貴族風格']
        upload_df['uniquefloat_012'] = read_equ['動物風格']
        upload_df['uniquefloat_013'] = read_equ['怪物風格']
        upload_df['uniquefloat_014'] = read_equ['角色風格']
        upload_df['uniquefloat_015'] = read_equ['自然風格']
        upload_df['uniquefloat_016'] = read_equ['重裝風格']
        upload_df['uniquefloat_017'] = read_equ['運動風格']
        upload_df['uniquefloat_018'] = read_equ['搞怪風格']
        upload_df['uniquefloat_019'] = read_equ['不擋身/腿']
        upload_df['uniquefloat_020'] = read_equ['不擋住名字']
        upload_df['uniquefloat_021'] = read_equ['渲染特效']

        return "MakeRawDataFileInsertOverwrite", upload_df , {}
