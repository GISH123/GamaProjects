import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

class UseModel_PredictY() :

    @classmethod
    def MakeUseModel_PredictY_M0_9001_99(self, makeInfo):
        # return "MakeUseModelFreeFuction", None, {}
        orderSQL1 = f"""
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:ModelVersion]_1' , dt = '[:DateNoLine]' , step = 'ModelResult')
            SELECT
                commondata_001
                , commondata_002
                , commondata_003
                , commondata_004
                , commondata_005
                , commondata_006
                , commondata_007
                , commondata_008
                , commondata_009
                , commondata_010
                , commondata_011
                , commondata_012
                , commondata_013
                , commondata_014
                , commondata_015
                , [:UniqueFloat]
                , UniqueJson_001 
            FROM gtwpd.model_usedata  
            WHERE 1=1
                AND product = 'maple'
                AND project='PredictY'
                AND version='P0_9001_1'
                AND dt = {makeInfo['makeTime'].replace('-', '')};
        """
        for tagInd_ in range(200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:UniqueFloat]", f"UniqueFloat_{columnOrder} \n\t\t\t\t, [:UniqueFloat]")
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t, [:UniqueFloat]", f"")

        orderSQL2 = f"""
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:ModelVersion]_2' , dt = '[:DateNoLine]' , step = 'ModelResult')
            SELECT
                commondata_001
                , commondata_002
                , commondata_003
                , commondata_004
                , commondata_005
                , commondata_006
                , commondata_007
                , commondata_008
                , commondata_009
                , commondata_010
                , commondata_011
                , commondata_012
                , commondata_013
                , commondata_014
                , commondata_015
                , CASE WHEN UniqueFloat_001 > 0 THEN 1 ELSE 0 END AS UniqueFloat_001
                , [:UniqueFloat]
                , UniqueJson_001 
            FROM gtwpd.model_usedata  
            WHERE 1=1
                AND product = 'maple'
                AND project='PredictY'
                AND version='P0_9001_1'
                AND dt = {makeInfo['makeTime'].replace('-', '')};
        """
        for tagInd_ in range(1, 200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL2 = orderSQL2.replace("[:UniqueFloat]", f"UniqueFloat_{columnOrder} \n\t\t\t\t, [:UniqueFloat]")
        orderSQL2 = orderSQL2.replace("\n\t\t\t\t, [:UniqueFloat]", f"")

        return "MakeUseModelOrderSQLInsert", [orderSQL1, orderSQL2] , {}
