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

class PreProcess_CheckProject() :

    @classmethod
    def auction16509Raw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
                    SELECT
                         *
                    FROM(
                        SELECT 
                            uniquestr_1, ROW_NUMBER() OVER(ORDER by rand()) as rn
                        FROM gtwpd.modelextract_modelextract AA
                        WHERE 1 = 1
                            AND AA.game = 'maple'
                            AND AA.tablenumber in ('16509')
                            AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                            AND AA.dt < {ed_dt.strftime('%Y%m%d')}
                    ) raw
                     where 1=1 
                        AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_auction16509Raw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def auction16508Raw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
                SELECT
                     *
                FROM(
                    SELECT 
                        uniquestr_1
                        , ROW_NUMBER() OVER(ORDER by rand()) as rn
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE 1 = 1
                        AND AA.game = 'maple'
                        AND AA.tablenumber in ('16508')
                        AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                        AND AA.dt < {ed_dt.strftime('%Y%m%d')}
                ) raw
                 where 1=1 
                    AND raw.rn <=10000
            '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_auction16508Raw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def exchange16608Raw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    uniquestr_1, ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('16608')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
            '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_exchange16608Raw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def exchange16609Raw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    uniquestr_1, ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('16609')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_exchange16609Raw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def fightRaw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    UniqueInt_1
                    , UniqueInt_1
                    , UniqueInt_3
                    , UniqueInt_5
                    , UniqueInt_6
                    , UniqueInt_7
                    , UniqueInt_8
                    , UniqueInt_12
                    , UniqueInt_13
                    , ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('2111')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_fightRaw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def friendRaw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    uniqueint_1, ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('15009')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_friendRaw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def growthRaw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    uniqueint_3, ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('2002')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_growthRaw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def guildRaw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
        SELECT
             *
        FROM(
            SELECT 
                UniqueInt_1
                , UniqueInt_4
                , ROW_NUMBER() OVER(ORDER by rand()) as rn
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = 'maple'
                AND AA.tablenumber in ('15109')
                AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                AND AA.dt < {ed_dt.strftime('%Y%m%d')}
        ) raw
         where 1=1 
            AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_guildRaw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def rankUnionRaw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    uniqueint_1, ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('17001')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
                '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_rankUnionRaw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def questRaw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    uniquestr_2, ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('4009')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
            '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_questRaw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def Login1132Raw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    UniqueInt_1
                    , UniqueInt_2
                    , UniqueInt_3
                    , UniqueInt_4
                    , UniqueInt_5
                    , UniqueInt_6
                    , UniqueInt_7
                    , UniqueInt_8
                    , UniqueInt_9
                    , UniqueInt_10
                    , UniqueInt_11
                    , ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('1132')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
        '''
        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_Login1132Raw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def Login1133Raw(self, st_dt, ed_dt, hiveCtrl):
        sql_str = f'''
            SELECT
                 *
            FROM(
                SELECT 
                    UniqueInt_1
                    , UniqueInt_2
                    , UniqueInt_3
                    , UniqueInt_4
                    , UniqueInt_5
                    , UniqueInt_6
                    , UniqueInt_7
                    , UniqueInt_8
                    , UniqueInt_9
                    , UniqueInt_10
                    , UniqueInt_11
                    , ROW_NUMBER() OVER(ORDER by rand()) as rn
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber in ('1133')
                    AND AA.dt >= {st_dt.strftime('%Y%m%d')}
                    AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            ) raw
             where 1=1 
                AND raw.rn <=10000
        '''

        print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        df.to_csv(f"/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/{ed_dt.strftime('%Y%m%d')}_Login1133Raw.csv")
        print(df)
        # hiveCtrl.executeSQL(sql_str)

    @classmethod
    def LoginDeteck(self, st_dt, ed_dt):
        path = "/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/"

        # detecting
        data = defaultdict(list)
        data["dt"].append(ed_dt.strftime('%Y%m%d'))
        detect1_all = []
        detect2_all = []
        for num_ in [1133, 1132]:
            df1 = pd.read_csv(path + f"{st_dt.strftime('%Y%m%d')}_Login{num_}Raw.csv").drop('Unnamed: 0', axis=1)
            df2 = pd.read_csv(path + f"{ed_dt.strftime('%Y%m%d')}_Login{num_}Raw.csv").drop('Unnamed: 0', axis=1)

            ## detect by column
            cols = df1.columns
            for col_ in cols:
                if col_.split('.')[1] == 'rn': continue
                detect1 = df1[col_].to_list()
                detect2 = df2[col_].to_list()
                statistic, pvalue = stats.ttest_ind(detect1, detect2)
                data[f"{col_}_{num_}"].append(pvalue)

                detect1_all.extend(detect1)
                detect2_all.extend(detect2)

        ## detect all
        statistic, pvalue = stats.ttest_ind(detect1_all, detect2_all)
        data[f"detect_all"].append(pvalue)

        # output
        detect_data_name = path + f"LoginDetect.csv"
        try:
            old_df = pd.read_csv(detect_data_name).drop('Unnamed: 0', axis=1)
        except:
            old_df = pd.DataFrame()

        new_df = pd.DataFrame.from_dict(data)
        output_df = pd.concat([old_df, new_df])
        output_df.to_csv(detect_data_name)

        # print(output_df)

    @classmethod
    def questDeteck(self, st_dt, ed_dt):
        path = "/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/"

        # detecting
        data = defaultdict(list)
        data["dt"].append(ed_dt.strftime('%Y%m%d'))
        df1 = pd.read_csv(path + f"{st_dt.strftime('%Y%m%d')}_questRaw.csv").drop('Unnamed: 0', axis=1)
        df2 = pd.read_csv(path + f"{ed_dt.strftime('%Y%m%d')}_questRaw.csv").drop('Unnamed: 0', axis=1)

        ## detect by column
        cols = df1.columns
        for col_ in cols:
            if col_.split('.')[1] == 'rn': continue
            detect1 = df1[col_].to_list()
            detect2 = df2[col_].to_list()
            detect1_count = {k_:v_ for k_, v_ in Counter(detect1).items() if v_ > 5}
            detect2_count = {k_:v_ for k_, v_ in Counter(detect2).items() if v_ > 5}
            keys = detect1_count.keys() & detect2_count.keys()

            detect_data = []
            for k_ in keys: detect_data.append([detect1_count[k_], detect2_count[k_]])
            chi2, p, dof, expected = stats.chi2_contingency(detect_data)
            # print(chi2, p, dof)

            data[col_].append(p)
        # print(data)

        # output
        detect_data_name = path + f"questDetect.csv"
        try:
            old_df = pd.read_csv(detect_data_name).drop('Unnamed: 0', axis=1)
        except:
            old_df = pd.DataFrame()

        new_df = pd.DataFrame.from_dict(data)
        output_df = pd.concat([old_df, new_df])
        output_df.to_csv(detect_data_name)

        # print(output_df)

    @classmethod
    def guildDeteck(self, st_dt, ed_dt):
        path = "/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/"

        # detecting
        data = defaultdict(list)
        data["dt"].append(ed_dt.strftime('%Y%m%d'))
        df1 = pd.read_csv(path + f"{st_dt.strftime('%Y%m%d')}_guildRaw.csv").drop('Unnamed: 0', axis=1)
        df2 = pd.read_csv(path + f"{ed_dt.strftime('%Y%m%d')}_guildRaw.csv").drop('Unnamed: 0', axis=1)

        ## detect by column
        # detect1 = df1['raw.uniqueint_4'].to_list()
        # detect2 = df2['raw.uniqueint_4'].to_list()
        # statistic, pvalue = stats.ttest_ind(detect1, detect2)
        # data['raw.uniqueint_4'].append(pvalue)

        detect1 = df1['raw.uniqueint_1'].to_list()
        detect2 = df2['raw.uniqueint_1'].to_list()
        detect1_count = {k_: v_ for k_, v_ in Counter(detect1).items() if v_ > 5}
        detect2_count = {k_: v_ for k_, v_ in Counter(detect2).items() if v_ > 5}
        keys = detect1_count.keys() & detect2_count.keys()
        detect_data = []
        for k_ in keys: detect_data.append([detect1_count[k_], detect2_count[k_]])
        chi2, p, dof, expected = stats.chi2_contingency(detect_data)
        data['raw.uniqueint_1'].append(p)
        print(data)
        # output
        detect_data_name = path + f"guildDetect.csv"
        try:
            old_df = pd.read_csv(detect_data_name).drop('Unnamed: 0', axis=1)
        except:
            old_df = pd.DataFrame()

        new_df = pd.DataFrame.from_dict(data)
        output_df = pd.concat([old_df, new_df])
        output_df.to_csv(detect_data_name)

        # print(output_df)

    @classmethod
    def growthDeteck(self, st_dt, ed_dt):
        path = "/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/"

        # detecting
        data = defaultdict(list)
        data["dt"].append(ed_dt.strftime('%Y%m%d'))
        df1 = pd.read_csv(path + f"{st_dt.strftime('%Y%m%d')}_growthRaw.csv").drop('Unnamed: 0', axis=1)
        df2 = pd.read_csv(path + f"{ed_dt.strftime('%Y%m%d')}_growthRaw.csv").drop('Unnamed: 0', axis=1)

        ## detect by column
        cols = df1.columns
        for col_ in cols:
            if col_.split('.')[1] == 'rn': continue
            detect1 = df1[col_].to_list()
            detect2 = df2[col_].to_list()
            statistic, pvalue = stats.ttest_ind(detect1, detect2)
            data[col_].append(pvalue)

        # output
        detect_data_name = path + f"growthDetect.csv"
        try:
            old_df = pd.read_csv(detect_data_name).drop('Unnamed: 0', axis=1)
        except:
            old_df = pd.DataFrame()

        new_df = pd.DataFrame.from_dict(data)
        output_df = pd.concat([old_df, new_df])
        output_df.to_csv(detect_data_name)

        # print(output_df)

    @classmethod
    def fightDeteck(self, st_dt, ed_dt):
        path = "/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/"

        # detecting
        data = defaultdict(list)
        data["dt"].append(ed_dt.strftime('%Y%m%d'))
        df1 = pd.read_csv(path + f"{st_dt.strftime('%Y%m%d')}_fightRaw.csv").drop('Unnamed: 0', axis=1)
        df2 = pd.read_csv(path + f"{ed_dt.strftime('%Y%m%d')}_fightRaw.csv").drop('Unnamed: 0', axis=1)

        ## detect by column
        cols = df1.columns
        for col_ in cols:
            if col_.split('.')[1] == 'rn': continue
            detect1 = df1[col_].to_list()
            detect2 = df2[col_].to_list()
            statistic, pvalue = stats.ttest_ind(detect1, detect2)
            data[col_].append(pvalue)

        # output
        detect_data_name = path + f"fightDetect.csv"
        try:
            old_df = pd.read_csv(detect_data_name).drop('Unnamed: 0', axis=1)
        except:
            old_df = pd.DataFrame()

        new_df = pd.DataFrame.from_dict(data)
        output_df = pd.concat([old_df, new_df])
        output_df.to_csv(detect_data_name)

        # print(output_df)

    @classmethod
    def plotDeteck(self):
        path = "/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/ModelData/P1_0_7/"
        df1 = pd.read_csv(path + "fightDetect.csv").drop('Unnamed: 0', axis=1)
        df2 = pd.read_csv(path + "LoginDetect.csv").drop('Unnamed: 0', axis=1)
        df3 = pd.read_csv(path + "growthDetect.csv").drop('Unnamed: 0', axis=1)
        df4 = pd.read_csv(path + "guildDetect.csv").drop('Unnamed: 0', axis=1)
        df5 = pd.read_csv(path + "questDetect.csv").drop('Unnamed: 0', axis=1)

        df1_np = df1.iloc[:,1:].to_numpy()
        df2_np = df2.iloc[:,1:].to_numpy()
        df3_np = df3.iloc[:,1:].to_numpy()
        df4_np = df4.iloc[:,1:].to_numpy()
        df5_np = df5.iloc[:,1:].to_numpy()

        df1.dt = df1.dt.astype(str)
        dfdt_list = df1.dt.unique()
        df1_list = []
        df2_list = []
        df3_list = []
        df4_list = []
        df5_list = []

        for _ in df1_np: df1_list.append(sum(_ < 0.01))
        for _ in df2_np: df2_list.append(sum(_ < 0.01))
        for _ in df3_np: df3_list.append(sum(_ < 0.01))
        for _ in df4_np: df4_list.append(sum(_ < 0.01))
        for _ in df5_np: df5_list.append(sum(_ < 0.01))
        print(df1_list)
        print(df2_list)
        print(df3_list)
        print(df4_list)
        print(df5_list)

        plt.subplot(4, 1, 1)
        plt.bar(dfdt_list, df1_list, width= 0.3)
        plt.subplot(4, 1, 2)
        plt.bar(dfdt_list, df2_list, width= 0.3)
        plt.subplot(4, 1, 3)
        plt.bar(dfdt_list, df3_list, width= 0.3)
        # plt.subplot(4, 1, 4)
        # plt.bar(dfdt_list, df4_list, width= 0.3)
        plt.subplot(4, 1, 4)
        plt.bar(dfdt_list, df5_list, width= 0.3)
        plt.show()

        plt.figure(figsize=(9, 7))
        plt.bar(dfdt_list, df1_list, label='角色能力Tag')
        plt.bar(dfdt_list, df2_list, bottom=np.array(df1_list), label='登入Tag')
        plt.bar(dfdt_list, df3_list, bottom=np.array(df1_list) + np.array(df2_list), label='成長Tag')
        plt.bar(dfdt_list, df4_list, bottom=np.array(df1_list) + np.array(df2_list) + np.array(df3_list), label='公會Tag')
        plt.bar(dfdt_list, df5_list, bottom=np.array(df1_list) + np.array(df2_list) + np.array(df3_list) + np.array(df4_list), label='任務Tag')
        plt.legend(loc='lower left', bbox_to_anchor=(0.8, 1.0))
        plt.title('輸入資料穩定度檢定')
        plt.show()

    ####################################################################################################################

    @classmethod
    def MakePreProcess_CheckProject_P1_0_1(self, makeInfo):
        """
        MakePreProcessCtrl 方法說明
            會撈取 return preprocessType, preprocessObject 的 preprocessType, preprocessObject 找到相關的檔案
            例如 preprocessType , preprocessObject 就會撈取 ExampleProduct\CheckProject\file\ctrl\ExampleProduct_{preprocessObject}_PreProcessCtrl.csv
        PreProcessCtrl 欄位說明
            new_col
            origin_col
            negtive
            do_log
            scale
            centering
            description
        """
        return "MakePreProcessCtrl", "CheckProject_P1_0_1"

    @classmethod
    def MakePreProcess_CheckProject_P1_0_2(self, makeInfo):
        """
        MakePreProcessOrderSQLInsert 方法說明
            會撈取 return preprocessType , preprocessObject 的 preprocessObject 裡面放置SQL陣列
            就會將 preprocessObject 裡的SQL陣列丟置HIVE執行
        相關可輸入參數
            ["[:ProductName]", productName]                                         # 產品名稱
            ["[:Project]", project]                                                 # 專案名稱
            ["[:ModelVersion]", modelVersion]                                       # 整體模型版本編號
            ["[:UseModelVersion]", usemodelVersion]                                 # 本次UseModel版本編號
            ["[:PreProcessVersion]", preprocessVersion]                             # 本次PreProcess版本編號
            ["[:RawDataVersion]", rawdataVersion]                                   # 本次RawData版本編號
            ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]                          # 資料製作日期 格式為%Y-%m-%d
            ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]                          #  資料製作日期 格式為%Y%m%d
            ["[:DateLineFirstMonth]", firstMonthTime.strftime("%Y-%m-%d")]          # 資料製作日期這個月的第一天 格式為 %Y-%m-%d
            ["[:DateLineLastMonth]", lastMonthTime.strftime("%Y-%m-%d")]            # 資料製作日期這個月的最後一天 格式為 %Y-%m-%d
            ["[:DateNoLineFirstMonth]", firstMonthTime.strftime("%Y%m%d")]          # 資料製作日期這個月的第一天 格式為 %Y%m%d
            ["[:DateNoLineLastMonth]", lastMonthTime.strftime("%Y%m%d")]            # 資料製作日期這個月的最後一天 格式為 %Y%m%d
        有些需要特定製作的日期，請在本Function先做好replace到SQL中
        """
        orderSQL1 = """   
        WITH maintable1 AS ( 
            SELECT   
                AA.CommonData_001
                , AA.CommonData_002
                , AA.CommonData_003
                , AA.CommonData_004
                , AA.CommonData_005
                , AA.CommonData_006
                , AA.CommonData_007
                , AA.CommonData_008
                , AA.CommonData_009
                , AA.CommonData_010
                , AA.CommonData_011
                , AA.CommonData_012
                , AA.CommonData_013
                , AA.CommonData_014
                , AA.CommonData_015
                , AA.UniqueFloat_001 AS UniqueFloat_001
                , AA.UniqueFloat_002 AS UniqueFloat_002         
            FROM gtwpd.model_usedata AA 
            WHERE 1 = 1  
                AND AA.product = '[:ProductName]'
                AND AA.project = '[:Project]'
                AND AA.step = 'RawData'  
                AND AA.version = '[:RawDataVersion]'
                AND AA.dt = '[:DateNoLine]'
         ), maintable2 AS ( 
            SELECT
                MAX(0) AS c_UniqueFloat_001 
                , MAX(1) AS v_UniqueFloat_001
                , MAX(0) AS c_UniqueFloat_002 
                , MAX(1) AS v_UniqueFloat_002 
            FROM maintable1
         ) INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]', version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess'  ) 
        SELECT
            CommonData_001
            , CommonData_002
            , CommonData_003
            , CommonData_004
            , CommonData_005
            , CommonData_006
            , CommonData_007
            , CommonData_008
            , CommonData_009
            , CommonData_010
            , CommonData_011
            , CommonData_012
            , CommonData_013
            , CommonData_014
            , CommonData_015
            , NVL(ROUND((UniqueFloat_001 - c_UniqueFloat_001) / v_UniqueFloat_001 ,6), 0) AS UniqueFloat_001
            , NVL(ROUND((UniqueFloat_002 - c_UniqueFloat_002) / v_UniqueFloat_002 ,6), 0) AS UniqueFloat_002
            , NULL AS UniqueFloat_003
            , NULL AS UniqueFloat_004
            , NULL AS UniqueFloat_005
            , NULL AS UniqueFloat_006
            , NULL AS UniqueFloat_007
            , NULL AS UniqueFloat_008
            , NULL AS UniqueFloat_009
            , NULL AS UniqueFloat_010
            , NULL AS UniqueFloat_011
            , NULL AS UniqueFloat_012
            , NULL AS UniqueFloat_013
            , NULL AS UniqueFloat_014
            , NULL AS UniqueFloat_015
            , NULL AS UniqueFloat_016
            , NULL AS UniqueFloat_017
            , NULL AS UniqueFloat_018
            , NULL AS UniqueFloat_019
            , NULL AS UniqueFloat_020
            , NULL AS UniqueFloat_021
            , NULL AS UniqueFloat_022
            , NULL AS UniqueFloat_023
            , NULL AS UniqueFloat_024
            , NULL AS UniqueFloat_025
            , NULL AS UniqueFloat_026
            , NULL AS UniqueFloat_027
            , NULL AS UniqueFloat_028
            , NULL AS UniqueFloat_029
            , NULL AS UniqueFloat_030
            , NULL AS UniqueFloat_031
            , NULL AS UniqueFloat_032
            , NULL AS UniqueFloat_033
            , NULL AS UniqueFloat_034
            , NULL AS UniqueFloat_035
            , NULL AS UniqueFloat_036
            , NULL AS UniqueFloat_037
            , NULL AS UniqueFloat_038
            , NULL AS UniqueFloat_039
            , NULL AS UniqueFloat_040
            , NULL AS UniqueFloat_041
            , NULL AS UniqueFloat_042
            , NULL AS UniqueFloat_043
            , NULL AS UniqueFloat_044
            , NULL AS UniqueFloat_045
            , NULL AS UniqueFloat_046
            , NULL AS UniqueFloat_047
            , NULL AS UniqueFloat_048
            , NULL AS UniqueFloat_049
            , NULL AS UniqueFloat_050
            , NULL AS UniqueFloat_051
            , NULL AS UniqueFloat_052
            , NULL AS UniqueFloat_053
            , NULL AS UniqueFloat_054
            , NULL AS UniqueFloat_055
            , NULL AS UniqueFloat_056
            , NULL AS UniqueFloat_057
            , NULL AS UniqueFloat_058
            , NULL AS UniqueFloat_059
            , NULL AS UniqueFloat_060
            , NULL AS UniqueFloat_061
            , NULL AS UniqueFloat_062
            , NULL AS UniqueFloat_063
            , NULL AS UniqueFloat_064
            , NULL AS UniqueFloat_065
            , NULL AS UniqueFloat_066
            , NULL AS UniqueFloat_067
            , NULL AS UniqueFloat_068
            , NULL AS UniqueFloat_069
            , NULL AS UniqueFloat_070
            , NULL AS UniqueFloat_071
            , NULL AS UniqueFloat_072
            , NULL AS UniqueFloat_073
            , NULL AS UniqueFloat_074
            , NULL AS UniqueFloat_075
            , NULL AS UniqueFloat_076
            , NULL AS UniqueFloat_077
            , NULL AS UniqueFloat_078
            , NULL AS UniqueFloat_079
            , NULL AS UniqueFloat_080
            , NULL AS UniqueFloat_081
            , NULL AS UniqueFloat_082
            , NULL AS UniqueFloat_083
            , NULL AS UniqueFloat_084
            , NULL AS UniqueFloat_085
            , NULL AS UniqueFloat_086
            , NULL AS UniqueFloat_087
            , NULL AS UniqueFloat_088
            , NULL AS UniqueFloat_089
            , NULL AS UniqueFloat_090
            , NULL AS UniqueFloat_091
            , NULL AS UniqueFloat_092
            , NULL AS UniqueFloat_093
            , NULL AS UniqueFloat_094
            , NULL AS UniqueFloat_095
            , NULL AS UniqueFloat_096
            , NULL AS UniqueFloat_097
            , NULL AS UniqueFloat_098
            , NULL AS UniqueFloat_099
            , NULL AS UniqueFloat_100
            , NULL AS UniqueFloat_101
            , NULL AS UniqueFloat_102
            , NULL AS UniqueFloat_103
            , NULL AS UniqueFloat_104
            , NULL AS UniqueFloat_105
            , NULL AS UniqueFloat_106
            , NULL AS UniqueFloat_107
            , NULL AS UniqueFloat_108
            , NULL AS UniqueFloat_109
            , NULL AS UniqueFloat_110
            , NULL AS UniqueFloat_111
            , NULL AS UniqueFloat_112
            , NULL AS UniqueFloat_113
            , NULL AS UniqueFloat_114
            , NULL AS UniqueFloat_115
            , NULL AS UniqueFloat_116
            , NULL AS UniqueFloat_117
            , NULL AS UniqueFloat_118
            , NULL AS UniqueFloat_119
            , NULL AS UniqueFloat_120
            , NULL AS UniqueFloat_121
            , NULL AS UniqueFloat_122
            , NULL AS UniqueFloat_123
            , NULL AS UniqueFloat_124
            , NULL AS UniqueFloat_125
            , NULL AS UniqueFloat_126
            , NULL AS UniqueFloat_127
            , NULL AS UniqueFloat_128
            , NULL AS UniqueFloat_129
            , NULL AS UniqueFloat_130
            , NULL AS UniqueFloat_131
            , NULL AS UniqueFloat_132
            , NULL AS UniqueFloat_133
            , NULL AS UniqueFloat_134
            , NULL AS UniqueFloat_135
            , NULL AS UniqueFloat_136
            , NULL AS UniqueFloat_137
            , NULL AS UniqueFloat_138
            , NULL AS UniqueFloat_139
            , NULL AS UniqueFloat_140
            , NULL AS UniqueFloat_141
            , NULL AS UniqueFloat_142
            , NULL AS UniqueFloat_143
            , NULL AS UniqueFloat_144
            , NULL AS UniqueFloat_145
            , NULL AS UniqueFloat_146
            , NULL AS UniqueFloat_147
            , NULL AS UniqueFloat_148
            , NULL AS UniqueFloat_149
            , NULL AS UniqueFloat_150
            , NULL AS UniqueFloat_151
            , NULL AS UniqueFloat_152
            , NULL AS UniqueFloat_153
            , NULL AS UniqueFloat_154
            , NULL AS UniqueFloat_155
            , NULL AS UniqueFloat_156
            , NULL AS UniqueFloat_157
            , NULL AS UniqueFloat_158
            , NULL AS UniqueFloat_159
            , NULL AS UniqueFloat_160
            , NULL AS UniqueFloat_161
            , NULL AS UniqueFloat_162
            , NULL AS UniqueFloat_163
            , NULL AS UniqueFloat_164
            , NULL AS UniqueFloat_165
            , NULL AS UniqueFloat_166
            , NULL AS UniqueFloat_167
            , NULL AS UniqueFloat_168
            , NULL AS UniqueFloat_169
            , NULL AS UniqueFloat_170
            , NULL AS UniqueFloat_171
            , NULL AS UniqueFloat_172
            , NULL AS UniqueFloat_173
            , NULL AS UniqueFloat_174
            , NULL AS UniqueFloat_175
            , NULL AS UniqueFloat_176
            , NULL AS UniqueFloat_177
            , NULL AS UniqueFloat_178
            , NULL AS UniqueFloat_179
            , NULL AS UniqueFloat_180
            , NULL AS UniqueFloat_181
            , NULL AS UniqueFloat_182
            , NULL AS UniqueFloat_183
            , NULL AS UniqueFloat_184
            , NULL AS UniqueFloat_185
            , NULL AS UniqueFloat_186
            , NULL AS UniqueFloat_187
            , NULL AS UniqueFloat_188
            , NULL AS UniqueFloat_189
            , NULL AS UniqueFloat_190
            , NULL AS UniqueFloat_191
            , NULL AS UniqueFloat_192
            , NULL AS UniqueFloat_193
            , NULL AS UniqueFloat_194
            , NULL AS UniqueFloat_195
            , NULL AS UniqueFloat_196
            , NULL AS UniqueFloat_197
            , NULL AS UniqueFloat_198
            , NULL AS UniqueFloat_199
            , NULL AS UniqueFloat_200
            , NULL AS UniqueJson_001
        FROM maintable1 
        JOIN maintable2 ; 
                """
        return "MakePreProcessOrderSQLInsert", [orderSQL1]

    @classmethod
    def MakePreProcess_CheckProject_P1_0_3(self, makeInfo):
        """
        MakePreProcessFileInsert 方法說明
            會撈取 return preprocessType , preprocessObject 的 preprocessObject 裡面放置DF資料
            就會將 preprocessObject 裡的DF上傳至HDFS上面，並且利用Hive建立與HDFS的Partition

            上傳路徑為 /user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/version=[:Version]/dt=[:DateNoLine]/step=[:Step]
            像是該Function 就會上傳到/user/GTW_PD/DB/Model/Usedata/product=ExampleProduct/project=CheckProject/version=P1_0_3/dt=20211231/step=PreProcess
        """
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        sqlStr = f'''
                           SELECT 
                               project 
                               , dt
                               , [:uniquefloat]
                           FROM
                               gtwpd.model_usedata 
                           WHERE 1=1 
                               AND product='[:ProductName]'
                               AND project = '[:model]'
                               AND step = 'preprocess'
                               AND version = '2_0_0'
                               AND [:DateRange]
                       '''
        # for model_ in ['auction', 'exchange', 'fight', 'friend', 'guild', 'growth', 'login']:
        model_ = 'login'
        doSqlStr = sqlStr \
            .replace('[:ProductName]', f'maple') \
            .replace('[:model]', f'{model_}tag') \
            .replace('[:DateRange]', 'dt >= 20191201 AND dt < 20200101')
        for _ in range(1):
            doSqlStr = doSqlStr.replace('[:uniquefloat]', f'uniquefloat_{(_ + 1):03},\n\t\t\t\t[:uniquefloat]')
        doSqlStr = doSqlStr.replace(',\n\t\t\t\t[:uniquefloat]', '')
        print(doSqlStr)
        hiveCtrl.executeSQL(doSqlStr)

        df = hiveCtrl.searchSQL(doSqlStr)
        print(df.head())
        df.to_csv('/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/loginCol1.csv')
        return "MakePreProcessFileInsert", df

    @classmethod
    def MakePreProcess_CheckProject_P1_0_4(self, makeInfo):
        """
        MakePreProcessFreeFuction
            各種處理方式，隨意，最後請自行上傳到HIVE
        範例說明：
            以下是相關的自由Fuction資料製作
        """

        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        dtList = [
            # "20191201", "20200101", "20200201", "20200301", "20200401", "20200501", "20200601", "20200701",
            # "20200801", "20200901", "20201001", "20201101", "20201201", "20210101", "20210201", "20210301",
            # "20210401", "20210501",
            "20210601", "20210701", "20210801", "20210901", "20211001", "20211101",
            "20211201", "20220101", "20220201", "20220301", "20220401"
        ]
        st = 0
        ed = 1
        while ed < len(dtList):
            hiveCtrl.executeSQL(f'DROP TABLE IF EXISTS gtwpd.WPU_TMP_{dtList[ed]}')
            sqlCreateStr = f'''
                 CREATE TABLE IF NOT EXISTS gtwpd.WPU_TMP{dtList[ed]} AS
                 SELECT
                     *
                 FROM(
                     SELECT
                         * ,
                         ROW_NUMBER() OVER(PARTITION BY project, dt ORDER by rand()) as rn
                     FROM
                         gtwpd.model_usedata
                     WHERE 1=1
                         AND product='maple'
                         AND step = 'preprocess'
                         AND version = '2_0_0'
                         AND dt >= {dtList[st]} AND dt <= {dtList[ed]}
                 ) raw
                 where 1=1
                     AND raw.rn <=100000
             '''
            hiveCtrl.executeSQL(sqlCreateStr)

            sqlDFStr = '''SELECT * FROM gtwpd.WPU_TMP'''
            hiveCtrl.searchSQL(sqlDFStr).to_csv(
                f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[ed]}tmp_.csv')

            st += 1
            ed += 1

        return "MakePreProcessFreeFuction", None

    @classmethod
    def MakePreProcess_CheckProject_P1_0_5(self, makeInfo):
        """
        MakePreProcessFreeFuction
            各種處理方式，隨意，最後請自行上傳到HIVE
        範例說明：
            以下是相關的自由Fuction資料製作
        """

        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        dtList = [
            "20210601", "20210701", "20210801", "20210901", "20211001", "20211101",
            "20211201", "20220101", "20220201", "20220301", "20220401"
        ]
        st = 0
        ed = 1
        while ed < len(dtList):
            sqlDFStr = f'''SELECT * FROM gtwpd.WPU_TMP{dtList[ed]}'''
            print(sqlDFStr)
            hiveCtrl.searchSQL(sqlDFStr).to_csv(
                f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[ed]}tmp_.csv')

            st += 1
            ed += 1
        return
        st = 0
        ed = 1
        while ed < len(dtList):

            # 讀檔案
            df = pd.read_csv(
                f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[ed]}tmp_.csv').drop(
                'Unnamed: 0', axis=1)
            cols = [f'CommonData_{(_ + 1):03}' for _ in range(15)] \
                   + [f'UniqueFloat_{(_ + 1):03}' for _ in range(200)] \
                   + ['uniquejson_001', 'product','project', 'step', 'version', 'dt', 'rn']

            print(df.columns)
            df.columns = cols
            df = df.drop(['product', 'step', 'version', 'rn'], axis=1)

            # 計算每個模型的樣本大小
            df_project_count_dict = df.groupby(['project', 'dt']).size().to_dict()
            print(df_project_count_dict)

            # 取的每個欄位的rank值 groupby project
            n = 3
            for col_ in [f'UniqueFloat_{(_ + 1):03}' for _ in range(n)]:
                df[col_ + '_rank'] = df.groupby('project')[col_].rank('average', ascending=True)

            # 加總每個欄位的rank值 groupby project dt
            col = ['project', 'dt'] + [f'UniqueFloat_{(_ + 1):03}_rank' for _ in range(n)]
            df_np = df[col].groupby(['project', 'dt']).sum().reset_index().to_numpy()
            print(df_np[0,:])

            # 計算每個 project
            sp = 0
            fp = 1
            while fp < df_np.shape[0]:
                project = df_np[sp, 0]
                dt1 = df_np[sp, 1]
                dt2 = df_np[fp, 1]
                N1 = df_project_count_dict[(project, dt1)]
                N2 = df_project_count_dict[(project, dt2)]
                AVG = N1 * N2 / 2
                sigma = (N1 * N2 * (N1 + N2 + 1) / 12) ** 0.5

                # 計算每個欄位
                ind_ = 2
                while ind_ < df_np.shape[1]:
                    SUM1 = df_np[sp, ind_]
                    SUM2 = df_np[fp, ind_]
                    U1 = N1 * N2 + N1 * (N1 + 1) / 2 - SUM1
                    U2 = N1 * N2 + N2 * (N2 + 1) / 2 - SUM2
                    Z = (min(U1, U2) - AVG) / sigma
                    print(project, col[ind_], dt1, dt2, N1, N2, SUM1, SUM2, U1, U2, Z)
                    ind_ += 1
                sp += 2
                fp += 2
            return
            st += 1
            ed += 1

        #
        # while dt_ft < len(dtList):
        #     df = pd.read_csv(f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[dt_ft]}_dataCheck_col1.csv').drop('Unnamed: 0', axis=1).set_index(['project', 'dt'])
        #     for col_ind_ in range(6, 201, 5):
        #         filePath = f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[dt_ft]}_dataCheck_col{col_ind_}.csv'
        #         if not exists(filePath): continue
        #
        #         df = pd.read_csv(filePath).drop('Unnamed: 0', axis=1).set_index(['project', 'dt'])
        #         print(df)
        #         df_all = pd.concat([df_all, df])
        #
        #     dt_sl += 1
        #     dt_ft += 1
        # print(df_all)

        return "MakePreProcessFreeFuction", None

    @classmethod
    def MakePreProcess_CheckProject_P1_0_6(self, makeInfo):
        """
        MakePreProcessFreeFuction
            各種處理方式，隨意，最後請自行上傳到HIVE
        範例說明：
            以下是相關的自由Fuction資料製作
        """

        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        dtList = [
            "20210601", "20210701", "20210801", "20210901",
            "20211001", #"20211101", "20211201", "20220101", "20220201", "20220301", "20220401"
        ]
        st = 0
        ed = 1
        df_data = defaultdict(list)
        while ed < len(dtList):

            # 讀檔案
            df = pd.read_csv(
                f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[ed]}tmp_.csv').drop(
                'Unnamed: 0', axis=1)
            cols = [f'CommonData_{(_ + 1):03}' for _ in range(15)] \
                   + [f'UniqueFloat_{(_ + 1):03}' for _ in range(200)] \
                   + ['uniquejson_001', 'product','project', 'step', 'version', 'dt', 'rn']

            print(df.columns)
            df.columns = cols
            df = df.drop(['product', 'step', 'version', 'rn'], axis=1)

            # 計算每個模型的樣本大小
            df_project_count_dict = df.groupby(['project', 'dt']).size().to_dict()
            print(df_project_count_dict)

            # 取的每個欄位的rank值 groupby project
            n = 3
            for col_ in [f'UniqueFloat_{(_ + 1):03}' for _ in range(n)]:
                df[col_ + '_rank'] = df.groupby('project')[col_].rank('average', ascending=True)

            # 加總每個欄位的rank值 groupby project dt
            col = ['project', 'dt'] + [f'UniqueFloat_{(_ + 1):03}_rank' for _ in range(n)]
            df_np = df[col].groupby(['project', 'dt']).sum().reset_index().to_numpy()
            print(df_np[0,:])

            # 計算每個 project
            sp = 0
            fp = 1
            while fp < df_np.shape[0]:
                project = df_np[sp, 0]
                # if project != 'guildtag' :
                #     sp += 2
                #     fp += 2
                #     continue

                dt1 = df_np[sp, 1]
                dt2 = df_np[fp, 1]
                N1 = df_project_count_dict[(project, dt1)]
                N2 = df_project_count_dict[(project, dt2)]
                AVG = N1 * N2 / 2
                sigma = (N1 * N2 * (N1 + N2 + 1) / 12) ** 0.5

                # 計算每個欄位
                df_data[(project, dt2)].append(project)
                df_data[(project, dt2)].append(dt2)
                ind_ = 2
                while ind_ < df_np.shape[1]:
                    SUM1 = df_np[sp, ind_]
                    SUM2 = df_np[fp, ind_]
                    U1 = N1 * N2 + N1 * (N1 + 1) / 2 - SUM1
                    U2 = N1 * N2 + N2 * (N2 + 1) / 2 - SUM2
                    Z = (min(U1, U2) - AVG) / sigma

                    df_data[(project, dt2)].append(Z)
                    print(project, col[ind_], dt1, dt2, N1, N2, SUM1, SUM2, U1, U2, Z)
                    ind_ += 1
                sp += 2
                fp += 2

            st += 1
            ed += 1
        cols = ['project', 'date'] + [f'UniqueFloat_{(_ + 1):03}_test' for _ in range(n)]
        df = pd.DataFrame.from_dict(df_data, orient='index')
        print(df)

        #
        # while dt_ft < len(dtList):
        #     df = pd.read_csv(f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[dt_ft]}_dataCheck_col1.csv').drop('Unnamed: 0', axis=1).set_index(['project', 'dt'])
        #     for col_ind_ in range(6, 201, 5):
        #         filePath = f'/Users/PeiyuWU/Git/machinelearningsystem/maple/CheckProject/file/data/{dtList[dt_ft]}_dataCheck_col{col_ind_}.csv'
        #         if not exists(filePath): continue
        #
        #         df = pd.read_csv(filePath).drop('Unnamed: 0', axis=1).set_index(['project', 'dt'])
        #         print(df)
        #         df_all = pd.concat([df_all, df])
        #
        #     dt_sl += 1
        #     dt_ft += 1
        # print(df_all)

        return "MakePreProcessFreeFuction", None

    @classmethod
    def MakePreProcess_CheckProject_P1_0_7(self, makeInfo):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        dt_list = [
            "2021-06-01", "2021-07-01",
            "2021-08-01", "2021-09-01", "2021-10-01",  "2021-11-01", "2021-12-01",
            "2022-01-01", "2022-02-01", "2022-03-01", "2022-04-01"
        ]

        for dt in dt_list:
            st_dt = datetime.datetime.strptime(dt, "%Y-%m-%d") - datetime.timedelta(days=30)
            ed_dt = datetime.datetime.strptime(dt, "%Y-%m-%d")
            # self.rankUnionRaw(st_dt, ed_dt, hiveCtrl)
            # self.questRaw(st_dt, ed_dt, hiveCtrl)
            # self.Login1132Raw(st_dt, ed_dt, hiveCtrl)
            # self.Login1133Raw(st_dt, ed_dt, hiveCtrl)
            # self.guildRaw(st_dt, ed_dt, hiveCtrl)
            # self.growthRaw(st_dt, ed_dt, hiveCtrl)
            # self.friendRaw(st_dt, ed_dt, hiveCtrl)
            # self.fightRaw(st_dt, ed_dt, hiveCtrl)
            # self.exchange16608Raw(st_dt, ed_dt, hiveCtrl)
            # self.exchange16609Raw(st_dt, ed_dt, hiveCtrl)
            # self.auction16508Raw(st_dt, ed_dt, hiveCtrl)
            # self.auction16509Raw(st_dt, ed_dt, hiveCtrl)

        for _ in range(len(dt_list) - 1):
            st_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            ed_dt = datetime.datetime.strptime(dt_list[_+1], "%Y-%m-%d")
            # self.LoginDeteck(st_dt, ed_dt)
            # self.questDeteck(st_dt, ed_dt)
            # self.guildDeteck(st_dt, ed_dt)
            # self.growthDeteck(st_dt, ed_dt)
            # self.fightDeteck(st_dt, ed_dt)

        self.plotDeteck()




        return "MakePreProcessFreeFuction", None
