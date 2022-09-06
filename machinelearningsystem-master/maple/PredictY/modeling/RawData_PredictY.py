import os
import pandas as pd
import numpy as np
import datetime
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.common.RawPreModel import RawPreModel

class RawData_PredictY() :

    # 文本
    @classmethod
    def MakeRawData_PredictY_R0_9001_0(self, makeInfo):
        # return "MakeUseModelFreeFuction", None, {}
        def getEventList(hiveCtrl):
            sql_str = '''
                    SELECT DISTINCT 
                    date_format(commondata_006, 'yyyyMMdd') as st_date
                    , date_format(commondata_007, 'yyyyMMdd') as ed_date
                    FROM gtwpd.model_usedata  
                    WHERE 1=1
                        AND product = 'maple'
                        AND project='ExternalDataManage'
                        AND version='R1_0_1'
                        AND commondata_006 != ''
                    '''
            df = hiveCtrl.searchSQL(sql_str)
            dt_np = df[['st_date', 'ed_date']].drop_duplicates().to_numpy()
            return dt_np

        hiveCtrl = RawPreModel.getHiveCtrl()
        dt_np = getEventList(hiveCtrl)
        # print(dt_np)

        tagorder = 0
        tagDictionarys = []
        for ind_, _ in enumerate(dt_np):
            st_ = _[0]
            ed_ = _[1]
            day_num = datetime.datetime.strptime(ed_, "%Y%m%d") - datetime.datetime.strptime(st_, "%Y%m%d")
            for day_ in range(day_num.days+1):
                tagDictionary = []
                item_id = '5222123' if datetime.datetime.strptime(st_, "%Y%m%d") < datetime.datetime.strptime('20210908', "%Y%m%d") else '5680946'
                tagDictionary.append(f'{item_id}_{st_}')
                tagDictionary.append(f'{item_id}_{st_}')
                tagDictionary.append(ind_)
                tagDictionary.append('從 20201230 開始至今，紀錄每個人買 "時尚隨機箱檔期" 的百分水位')
                tagDictionary.append('{}')
                tagDictionary.append((datetime.datetime.strptime(st_, "%Y%m%d") + datetime.timedelta(days=day_)).strftime("%Y%m%d"))
                tagDictionary.append(item_id)
                tagDictionarys.append(tagDictionary)

        df = pd.DataFrame(tagDictionarys)
        df.columns = [
            "commondata_011"  # 標籤名稱(英)
            , "commondata_012"  # 標籤名稱(中)
            , "commondata_013"  # 標籤編號
            , "commondata_014"  # 標籤說明
            , "commondata_015"  # 標籤JSON
            , "commondata_007"  # 標籤編碼1
            , "commondata_008"  # 標籤編碼2
        ]
        print(df)
        return "MakeRawDataFileInsertOverwrite", df, {}

    @classmethod
    def MakeRawData_PredictY_R0_9001_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None , {}
        def getEventTime():
            tagSQL1 = f'''
                SELECT DISTINCT 
                    commondata_011 as tag_name
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='PredictY'
                    AND version='R0_9001_0'
                    AND dt = {makeInfo['makeTime'].replace('-', '')}
            '''
            hiveCtrl = RawPreModel.getHiveCtrl()
            tagDataNumpy = hiveCtrl.searchSQL(tagSQL1).tag_name.unique()
            tagTimeNumpy = np.array([datetime.datetime.strptime(_.split('_')[1], "%Y%m%d") for _ in tagDataNumpy])
            return tagTimeNumpy

        tagTimeNumpy = getEventTime()
        item_idx = np.where(tagTimeNumpy < datetime.datetime.strptime(makeInfo['makeTime'], "%Y-%m-%d"))[0][-1]
        makeTime = makeInfo['makeTime'].replace('-', '')
        dataTime1 = tagTimeNumpy[item_idx].strftime("%Y%m%d")
        dataTime2 = (datetime.datetime.strptime(makeInfo['makeTime'], "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        # print(tagTimeNumpy)
        # print(dataTime1, dataTime2)

        orderSQL1 = f"""
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:RawDataVersion]' , dt = '[:DateNoLine]' , step = 'RawData')
            SELECT
                CommonData_1 as commondata_001
                , null as commondata_002
                , null as commondata_003
                , null as commondata_004
                , null as commondata_005
                , null as commondata_006
                , date_format(UniqueTime_1, 'yyyyMMdd') AS commondata_007
                , CommonData_10 AS commondata_008
                , NULL AS commondata_009
                , NULL AS commondata_010
                , NULL AS commondata_011
                , NULL AS commondata_012
                , NULL AS commondata_013
                , NULL AS commondata_014
                , NULL AS commondata_015
                , UniqueInt_4 as UniqueFloat_001 
                , [:UniqueFloatNull]
                , NULL AS UniqueJson_001 
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = 'maple'
                AND AA.tablenumber = '16009'
                AND AA.dt >= {dataTime1}
                AND AA.dt <= {dataTime2}
                AND AA.UniqueStr_11 != 'RollBack'
                AND AA.commondata_10 in ('5222123', '5680946');
        """
        for tagInd_ in range(1, 200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:UniqueFloatNull]", f"NULL AS UniqueFloat_{columnOrder} \n\t\t\t\t, [:UniqueFloatNull]")
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t, [:UniqueFloatNull]", f"")

        return "MakeRawDataOrderSQLInsert", [orderSQL1], {'dataTime1': dataTime1, 'dataTime2': dataTime2}
