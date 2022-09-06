import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
import pandas as pd

class PreProcess_ModelEX() :

    @classmethod
    def CheckModleEX(self, hiveCtrl):
        table_list = ["1001", "1003", "6006", "6007", "1002", "1132", "1133"]
        for tb in table_list:
            st_dt = datetime.datetime.strptime("2022-05-01", "%Y-%m-%d")
            sql_str = f'''
                SELECT 
                    *
                FROM
                    gtwpd.modelextract_modelextract aa
                WHERE 1=1  
                    AND aa.game='maple' 
                    AND aa.dt == {st_dt.strftime('%Y%m%d')}
                    AND aa.tablenumber = {tb}
                LIMIT 1000
            '''
            print(tb)
            df = hiveCtrl.searchSQL(sql_str)
            df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/ModelEX/file/ModelData/P1_0_1/GTW_MS_LoginPlus_Tablenumber{tb}_{st_dt.strftime('%Y%m%d')[:10]}.csv")

    @classmethod
    def MakePreProcess_ModelEX_P1_0_1(self, makeInfo):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        # self.CheckModleEX(hiveCtrl)

        dt_list = ['2022-05-01']
        tb_list = ['21001','22001','22002','24001','25001','25002','26001','26002','27001']
        for dt in dt_list:
            st_dt = datetime.datetime.strptime(dt, "%Y-%m-%d")
            for tb in tb_list:
                sql_str = f'''
                    SELECT 
                        *
                    FROM
                        gtwpd.business_bureport aa
                    WHERE 1=1  
                        AND aa.game='maple' 
                        AND aa.dt == {st_dt.strftime('%Y%m%d')}
                        AND aa.tablenumber = {tb}
                    LIMIT 1000
                '''
                df = hiveCtrl.searchSQL(sql_str)
                df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/ModelEX/file/ModelData/P1_0_1/GTW_MS_Tag_Tablenumber{tb}_{st_dt.strftime('%Y%m%d')[:10]}.csv")


        return "MakePreProcessFreeFuction", None
