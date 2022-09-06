import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.common.RawPreModel import RawPreModel
import math
import numpy as np

class UseModel_TagFilter() :

    @classmethod
    def MakeUseModel_TagFilter_M0_9001_99(self, makeInfo, **parm_):
        hiveCtrl = RawPreModel.getHiveCtrl()
        def getMapTime(hiveCtrl, makeTime):
            tagSQL1 = f'''
                SELECT  
                    commondata_011 AS mapping_date
                    , count(commondata_001) AS POS_NUM
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='PredictY'
                    AND version='V0_9001_1_1'
                    AND dt = {makeTime}
                GROUP BY commondata_011
            '''
            df = hiveCtrl.searchSQL(tagSQL1)
            dataNP = df.to_numpy()
            mapTime = dataNP[0, 0]
            posNum = dataNP[0, 1]
            return mapTime, posNum

        makeTime = makeInfo['makeTime'].replace('-', '')
        mapTime, posNum = getMapTime(hiveCtrl, makeTime)

        orderSQL1 = f"""
            with tb_base as (
                SELECT * 
                FROM gtwpd.model_usedata
                where 1 = 1 
                    AND product = 'maple'
                    AND project = 'AutoTag'
                    AND version = 'V0_11001_1'
                    AND dt = {mapTime}         
            ), 
            tb_x1 as (
                SELECT * 
                FROM gtwpd.model_usedata
                where 1 = 1 
                    AND product = 'maple'
                    AND project = 'AutoTag'
                    AND version = 'V0_4001_1_1'
                    AND dt = {mapTime}         
            ),
            tb_y as (
                SELECT *
                FROM gtwpd.model_usedata
                WHERE 1=1
                    AND product = 'maple'
                    AND project='PredictY'
                    AND version='V0_9001_1_1'
                    AND dt = {makeTime}
            ),
            main as (
                SELECT 
                    base.commondata_001
                    , CASE WHEN y.uniquefloat_001 > 0 THEN 1 ELSE 0 END AS commondata_015
                    , [:tagNewColumn]
                FROM tb_base base
                LEFT JOIN tb_y y on base.commondata_001 = y.commondata_001 and base.dt = y.commondata_011
                LEFT JOIN tb_x1 x1 on base.commondata_001 = x1.commondata_001
            ),
            main_distinct as (
                SELECT 
                    commondata_001, commondata_015
                    , [:tagDistinctColumn]
                FROM main
                GROUP BY commondata_001, commondata_015
            )
            SELECT AA.* 
            FROM (
                SELECT * , ROW_NUMBER() OVER(PARTITION BY commondata_015 order by rand()) as rn
                FROM main_distinct
            ) AA
            WHERE AA.rn <= {posNum}
        """
        for tag_partition_ in range(4):
            for tag_order_ in range(200):
                raw_str1 = "[:tagNewColumn]"
                raw_str2 = "[:tagDistinctColumn]"
                replace_str1 = f"CASE WHEN x1.commondata_013 = {tag_partition_+1} THEN x1.UniqueFloat_{str(tag_order_+1).zfill(3)} END AS UniqueFloat_{str(tag_partition_ * 200 + tag_order_ + 1).zfill(3)}\n\t\t\t\t\t, [:tagNewColumn]"
                replace_str2 = f"MAX(UniqueFloat_{str(tag_partition_ * 200 + tag_order_ + 1).zfill(3)}) AS UniqueFloat_{str(tag_partition_ * 200 + tag_order_ + 1).zfill(3)} \n\t\t\t\t\t, [:tagDistinctColumn]"
                orderSQL1 = orderSQL1.replace(raw_str1, replace_str1)
                orderSQL1 = orderSQL1.replace(raw_str2, replace_str2)
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t\t, [:tagNewColumn]", f"")
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t\t, [:tagDistinctColumn]", f"")
        print(orderSQL1)

        df = hiveCtrl.searchSQL(orderSQL1)
        df.to_csv(f'D:/Git/machinelearningsystem/maple/TagFilter/file/ModelData/M_0_9001_1/train{makeTime}.csv')
        print(df)

        return "MakeUseModelFreeFuction", None, {}
