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

class PreProcess_ExternalDataManage() :
    ####################################################################################################################

    @classmethod
    def MakePreProcess_ExternalDataManage_P1_0_1(self, makeInfo):
        def getFashionBoxBUInfo(hiveCtrl):
            sql_str = '''
                    SELECT 
                        commondata_001, commondata_002, commondata_003, commondata_004, commondata_005, commondata_006
                        , commondata_007, commondata_008, uniquefloat_001, uniquefloat_002, uniquefloat_003, uniquefloat_004
                        , uniquefloat_005, uniquefloat_006, uniquefloat_007, uniquefloat_008, uniquefloat_009
                        , uniquefloat_010, uniquefloat_011, uniquefloat_012, uniquefloat_013, uniquefloat_014
                        , uniquefloat_015, uniquefloat_016, uniquefloat_017, uniquefloat_018, uniquefloat_019
                        , uniquefloat_020, uniquefloat_021
                    FROM gtwpd.model_usedata  
                    WHERE 1=1
                        AND product = 'maple'
                        AND project='ExternalDataManage'
                        AND version='R1_0_1'
                        AND commondata_006 != ''
                    '''
            print(sql_str)
            cols = [
                'version', 'Part', 'ItemID', 'Name', 'KR Name', 'Date1', 'Date2', '客觀概述'
                , 'Prob.', '市價', '大師標籤', 'IP合作'
                , '漂浮特效', '暗色系', '螢光系', '柔和色系', '可愛風格'
                , '制服風格', '貴族風格', '動物風格', '怪物風格', '角色風格'
                , '自然風格', '重裝風格', '運動風格', '搞怪風格', '不擋身/腿'
                , '不擋住名字', '渲染特效'
            ]
            df = hiveCtrl.searchSQL(sql_str)
            df.columns = cols
            return df
        # return "MakePreProcessFreeFuction", None

        hiveCtrl = RawPreModel.getHiveCtrl()
        df = getFashionBoxBUInfo(hiveCtrl)
        meltcols = [
            '大師標籤', 'IP合作'
            , '漂浮特效', '暗色系', '螢光系', '柔和色系', '可愛風格'
            , '制服風格', '貴族風格', '動物風格', '怪物風格', '角色風格'
            , '自然風格', '重裝風格', '運動風格', '搞怪風格', '不擋身/腿'
            , '不擋住名字', '渲染特效'
        ]

        df['客觀概述'] = df['客觀概述'].apply(lambda x: x.split('、'))
        eventWithoutward = df[['ItemID','客觀概述']].explode(['客觀概述'])
        eventWithoutward = eventWithoutward.loc[eventWithoutward['客觀概述'] != ''].reset_index(drop=True)
        ItemWithConcept = pd.melt(df, id_vars=['ItemID'], value_vars=meltcols)
        eventWithItem = df.loc[:, ["Date1", "ItemID", "Prob."]].reset_index(drop=True)

        eventWithoutward.columns = ['entity1', 'entity2']
        ItemWithConcept.columns = ['entity1', 'entity2', 'num']
        eventWithItem.columns = ['entity1', 'entity2', 'num']

        eventWithoutward['relation'] = '外觀'
        eventWithoutward['num'] = 1.0
        ItemWithConcept['relation'] = '是'
        eventWithItem['relation'] = '產生'

        ItemWithConcept = ItemWithConcept.loc[ItemWithConcept.num != 0].reset_index(drop=True)

        boxIndex1 = eventWithItem['entity1'] < '2021-09-08'
        boxIndex2 = eventWithItem['entity1'] >= '2021-09-08'
        eventWithItem['entity1'] = pd.to_datetime(eventWithItem['entity1']).dt.strftime('%Y-%m-%d')
        eventWithItem.loc[boxIndex1, 'entity1'] = '5222123_' + eventWithItem.loc[boxIndex1, 'entity1']
        eventWithItem.loc[boxIndex2, 'entity1'] = '5680946_' + eventWithItem.loc[boxIndex2, 'entity1']

        kgGraph = pd.concat([ItemWithConcept, eventWithItem, eventWithoutward])

        upload_df = RawPreModel.getUploadDataFrame()
        upload_df['commondata_001'] = kgGraph['entity1']
        upload_df['commondata_002'] = kgGraph['relation']
        upload_df['commondata_003'] = kgGraph['entity2']
        upload_df['uniquefloat_001'] = kgGraph['num']

        return "MakePreProcessFileInsertOverwrite", upload_df, {}
