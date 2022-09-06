import os
import pandas as pd
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
import csv
from tqdm import tqdm
from os.path import exists
import math
from collections import defaultdict
from scipy import stats
from collections import Counter
import matplotlib
import matplotlib.pyplot as plt
from scipy.stats import kde
import numpy as np
from package.common.common.RawPreModel import RawPreModel

class PreProcess_PredictY() :
    ####################################################################################################################
    # Tag - RANK()
    @classmethod
    def MakePreProcess_PredictY_P0_9001_1(self, makeInfo):
        # return "MakePreProcessFreeFuction", None , {}
        makeTime = makeInfo['makeTime'].replace('-', '')
        dataTime1 = makeInfo["result"]["rawdata"]['dataTime1']
        dataTime2 = makeInfo["result"]["rawdata"]['dataTime2']

        orderSQL1 = f"""
            WITH tb0 AS (
                SELECT 
                    commondata_001
                    , commondata_007
                    , commondata_008
                    , UniqueFloat_001 
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='PredictY'
                    AND version='R0_9001_1'
                    AND dt = {makeTime}
            ),
            tb1 AS( 
                SELECT 
                commondata_011   -- Tag
                , commondata_007 -- time
                , commondata_008 -- itemID
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='PredictY'
                    AND version='R0_9001_0'
                    AND dt = {makeTime}
             )
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
            SELECT 
                 aa.commondata_001 as commondata_001    -- 帳號
                , aa.commondata_002 as commondata_002    -- Tag
                , null as commondata_003
                , null as commondata_004
                , null as commondata_005
                , null as commondata_006
                , NULL AS commondata_007
                , NULL AS commondata_008
                , NULL AS commondata_009
                , NULL AS commondata_010
                , '{dataTime1}' AS commondata_011
                , '{dataTime2}' AS commondata_012
                , NULL AS commondata_013
                , NULL AS commondata_014
                , NULL AS commondata_015
                , CUME_DIST() OVER(PARTITION by aa.commondata_002 ORDER by aa.UniqueFloat_001)*0.9999 + 0.0001 as UniqueFloat_001
                , [:UniqueFloatNull]
                , NULL AS UniqueJson_001 
            FROM (
                SELECT 
                     a.commondata_001 as commondata_001    -- 帳號
                    , b.commondata_011 as commondata_002    -- Tag
                    , sum(a.UniqueFloat_001) AS UniqueFloat_001
                 FROM tb0 a
                 LEFT JOIN tb1 b on a.commondata_007 = b.commondata_007 and a.commondata_008 = b.commondata_008
                 WHERE b.commondata_011 IS NOT NULL
                 GROUP BY a.commondata_001, b.commondata_011 
             ) aa;
        """
        for tagInd_ in range(1, 200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:UniqueFloatNull]", f"NULL AS UniqueFloat_{columnOrder} \n\t\t\t\t, [:UniqueFloatNull]")
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t, [:UniqueFloatNull]", f"")

        # print(orderSQL1)
        return "MakePreProcessOrderSQLInsert", [orderSQL1], {}
