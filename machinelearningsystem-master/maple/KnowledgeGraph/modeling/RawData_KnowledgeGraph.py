import os
import pandas as pd
import numpy as np
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.common.RawPreModel import RawPreModel
from maple.KnowledgeGraph.modeling.old.RawData_KnowledgeGraph_OLD import RawData_KnowledgeGraph_OLD

class RawData_KnowledgeGraph(RawData_KnowledgeGraph_OLD) :

    @classmethod
    def getHiveCtrl(self):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        return hiveCtrl

    @classmethod
    def getLastOverEventRange(self, today, hiveCtrl):
        sql_str = '''
                SELECT DISTINCT commondata_006 as Date
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='R0_1_1'
                    AND commondata_006 != ''
                '''
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()
        # print(dt_list)

        today_dt = datetime.datetime.strptime(today, "%Y-%m-%d")
        dt_ind_ = 0
        for ind_ in range(0, len(dt_list) - 1):
            dt = datetime.datetime.strptime(dt_list[ind_], "%Y-%m-%d %H:%M:%S")
            if today_dt < dt: break

            dt_ind_ = ind_

        return (
            datetime.datetime.strptime(dt_list[dt_ind_- 1], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_], "%Y-%m-%d %H:%M:%S")
        )

    @classmethod
    def read_equipment_revised_ALL(self, Path):
        # read_equ = pd.read_excel(Path)
        read_equ = pd.read_csv(Path)
        # print(read_equ)
        read_equ = read_equ.drop(['Unnamed: 5'], axis=1)
        read_equ[['客觀概述']] = read_equ[['客觀概述']].fillna('')
        read_equ = read_equ.fillna(0)

        read_equ['event'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y/%m/%d')
        read_equ['Date'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y-%m-%d')
        read_equ = read_equ.reset_index(drop=True)

        return read_equ

    @classmethod
    def getEventList(self, hiveCtrl):
        sql_str = '''
                SELECT DISTINCT commondata_006 as Date
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='R0_1_1'
                    AND commondata_006 != ''
                '''
        # print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()[:-1]
        # print(dt_list)

        return dt_list

    @classmethod
    def getLastOverEvent(self, today, dt_list):

        today_dt = datetime.datetime.strptime(str(today), "%Y%m%d")
        dt_ind_ = 0
        for ind_ in range(0, len(dt_list) - 1):
            dt = datetime.datetime.strptime(dt_list[ind_], "%Y-%m-%d %H:%M:%S")
            if today_dt < dt: break

            dt_ind_ = ind_

        return datetime.datetime.strptime(dt_list[dt_ind_], "%Y-%m-%d %H:%M:%S")

    # 獲得最近兩期完成的檔期資料
    @classmethod
    def getLast2OverEventRange(self, today, hiveCtrl):
        sql_str = '''
                SELECT DISTINCT commondata_006 as Date
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='R0_1_1'
                    AND commondata_006 != ''
                '''
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()
        # print(dt_list)

        today_dt = datetime.datetime.strptime(today, "%Y-%m-%d")
        dt_ind_ = 0
        for ind_ in range(0, len(dt_list) - 1):
            dt = datetime.datetime.strptime(dt_list[ind_], "%Y-%m-%d %H:%M:%S")
            if today_dt < dt: break

            dt_ind_ = ind_

        return (
            datetime.datetime.strptime(dt_list[dt_ind_ - 2], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_ - 1], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_], "%Y-%m-%d %H:%M:%S")
        )

    @classmethod
    def getTrainValidEventRange(self, today, hiveCtrl):
        sql_str = '''
                    SELECT DISTINCT commondata_006 as Date
                    FROM gtwpd.model_usedata  
                    WHERE 1=1
                        AND product = 'maple'
                        AND project='KnowledgeGraph'
                        AND version='R0_1_1'
                        AND commondata_006 != ''
                    '''
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()
        # print(dt_list)

        today_dt = datetime.datetime.strptime(today, "%Y-%m-%d")
        dt_ind_ = 0
        for ind_ in range(0, len(dt_list) - 1):
            dt = datetime.datetime.strptime(dt_list[ind_], "%Y-%m-%d %H:%M:%S")
            if today_dt < dt: break

            dt_ind_ = ind_

        return (
            datetime.datetime.strptime(dt_list[dt_ind_ - 1], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_ + 1], "%Y-%m-%d %H:%M:%S")
        )

    # 建構檔期資料
    @classmethod
    def MakeRawData_KnowledgeGraph_R0_1_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None
        intPutPath='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv'

        # read_equ = pd.read_excel(Path)
        read_equ = pd.read_csv(intPutPath)
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
        for _ in range(len(dt_list) - 1):
            read_equ.loc[read_equ.Date == dt_list[_], "Date1"] = datetime.datetime.strptime(dt_list[_], '%Y-%m-%d')
            read_equ.loc[read_equ.Date == dt_list[_], "Date2"] = \
                datetime.datetime.strptime(dt_list[_ + 1], '%Y-%m-%d') + datetime.timedelta(days=-1)
        read_equ['Prob.'] = read_equ['Prob.'].str.replace('%','').astype(float)
        read_equ = read_equ.drop(['Date','主觀描述','類似檔期','類似檔期客觀概述','類似檔期主觀概述'], axis=1)
        print(read_equ.dtypes)
        print(pd.concat([read_equ.head(1), read_equ.tail(1)]))

        ################################################################################################################
        upload_df = RawPreModel.getUploadDataFrame()
        upload_df['commondata_001'] = makeInfo['parameter']['rawdata']['version']
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

        # print(upload_df)
        return "MakeRawDataFileInsertOverwrite", upload_df, {}

    @classmethod
    def MakeRawData_KnowledgeGraph_R1_1_1(self, makeInfo):
        return "MakeRawDataFreeFuction", None, {}

    # 拼湊圖譜資料
    @classmethod
    def MakeRawData_KnowledgeGraph_R2_1_2(self, makeInfo):
        def reorganizeKgUserIdPurchaseFashionBox(input_df, dt_list):
            ## 整資料
            data = []
            input_df_np = input_df.to_numpy()
            for _ in input_df_np:
                data.append(_)
                dt = _[-1]
                lastevent = self.getLastOverEvent(dt, dt_list)
                data[-1][2] = data[-1][2] + f"_{lastevent.strftime('%Y-%m-%d')}"

            data = np.array(data)
            df = pd.DataFrame(data, columns=['Entity1', 'Relation', 'Entity2', 'num', 'dt']).drop('dt', axis=1)
            df = df.groupby(['Entity1', 'Relation', 'Entity2'])['num'].count().reset_index()
            # df['dt'] = df['Entity2'].str.slice(start=-10)

            return df
        # return "MakeRawDataFreeFuction", None

        outputPath = 'D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_2'
        hiveCtrl = RawPreModel.getHiveCtrl()

        eventStartDate, eventEndDate = makeInfo['parameter']['makedate']
        eventStartDate = datetime.datetime.strptime(eventStartDate, "%Y-%m-%d")
        eventEndDate = datetime.datetime.strptime(eventEndDate, "%Y-%m-%d")
        # print(eventStartDate, eventEndDate)

        # Buy Box Graph
        sql_str = f'''
                        WITH
                        loginUser AS (
                           SELECT
                               commondata_1 as Entity1
                               , 'Login' as Relation
                               , 'maple' as Entity2
                               , sum(
                                   cast((unix_timestamp(UniqueTime_2, 'yyyy-mm-dd HH:mm:ss') - unix_timestamp(UniqueTime_1, 'yyyy-mm-dd HH:mm:ss')	) / 3600 as int)
                               ) as num
                           FROM
                               gtwpd.modelextract_modelextract
                           WHERE 1=1
                           AND game='maple'
                               AND dt >= {(eventEndDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                               AND dt < {eventEndDate.strftime('%Y%m%d')}
                               AND tablenumber = '1103'
                           GROUP BY commondata_1
                        )
                        SELECT
                            a.entity1, a.relation, a.entity2, a.num, a.dt
                        FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')} a
                        INNER JOIN loginUser b ON a.entity1 = b.entity1
                    '''
        print(sql_str)
        kgUserIdPurchaseFashionBox = hiveCtrl.searchSQL(sql_str)
        kgUserIdPurchaseFashionBox.columns = ['entity1', 'relation', 'entity2', 'num', 'dt']
        print(kgUserIdPurchaseFashionBox.loc[kgUserIdPurchaseFashionBox.entity1 == '0007946123'])
        '''
                               entity1 relation  entity2  num        dt
                        151854  0007946123       購買  5680946    1  20210908
                        151856  0007946123       購買  5680946    1  20210908
                        151859  0007946123       購買  5680946    1  20210908
                        151862  0007946123       購買  5680946    1  20210908
                        151863  0007946123       購買  5680946    1  20210908
                        152884  0007946123       購買  5680946    1  20210908
                        152885  0007946123       購買  5680946    1  20210908
                        206526  0007946123       購買  5680946    1  20211215
                        206527  0007946123       購買  5680946    1  20211215
                        245683  0007946123       購買  5680946    1  20211016
                        245684  0007946123       購買  5680946    1  20211016
                        245685  0007946123       購買  5680946    1  20211016
                        245686  0007946123       購買  5680946    1  20211016
                        245687  0007946123       購買  5680946    1  20211016
                        245785  0007946123       購買  5680946    1  20211016
                        245787  0007946123       購買  5680946    1  20211016
                        245788  0007946123       購買  5680946    1  20211016
                        245789  0007946123       購買  5680946    1  20211016
                        245790  0007946123       購買  5680946    1  20211016
                        275572  0007946123       購買  5680946    1  20211125
                    '''

        dt_list = self.getEventList(hiveCtrl)
        df = reorganizeKgUserIdPurchaseFashionBox(kgUserIdPurchaseFashionBox, dt_list)
        print(df.loc[df.Entity1 == '0007946123'])
        '''
              Entity1 Relation             Entity2  num          dt
        3  0007946123       購買  5680946_2021-09-08    7  2021-09-08
        4  0007946123       購買  5680946_2021-09-29   10  2021-09-29
        5  0007946123       購買  5680946_2021-11-17    1  2021-11-17
        6  0007946123       購買  5680946_2021-12-15    2  2021-12-15
        '''
        df.to_csv(outputPath + f"/KGRDFN_eventBuydf_{eventStartDate.strftime('%Y%m%d')}.csv")

        # main Graph
        sql_str = f'''
                WITH
                loginUser AS (
                   SELECT
                       commondata_1 as Entity1
                       , 'Login' as Relation
                       , 'maple' as Entity2
                       , sum(
                           cast((unix_timestamp(UniqueTime_2, 'yyyy-mm-dd HH:mm:ss') - unix_timestamp(UniqueTime_1, 'yyyy-mm-dd HH:mm:ss')	) / 3600 as int)
                       ) as num
                   FROM
                       gtwpd.modelextract_modelextract
                   WHERE 1=1
                   AND game='maple'
                       AND dt >= {(eventEndDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                       AND dt < {eventEndDate.strftime('%Y%m%d')}
                       AND tablenumber = '1103'
                   GROUP BY commondata_1
                ),
                purchaseUser AS (
                   SELECT
                       CommonData_1 as Entity1
                       , '購買' as Relation
                       , AA.CommonData_10 as Entity2
                       , COUNT(AA.CommonData_10) as num
                   FROM gtwpd.modelextract_modelextract AA
                   WHERE 1 = 1
                       AND AA.game = 'maple'
                       AND AA.tablenumber = '16009'
                       AND AA.dt >= {(eventEndDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                       AND AA.dt < {eventEndDate.strftime('%Y%m%d')}
                       AND AA.UniqueStr_1 = 'bf point'
                       AND AA.UniqueStr_11 != 'RollBack'
                       AND AA.commondata_10 IS NOT NULL
                       AND AA.commondata_10 != '0'
                   GROUP BY AA.CommonData_1, AA.CommonData_10
                ),
                onlyBuyFashionBoxUser AS (
                    SELECT DISTINCT Entity1
                    FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')}
                ),
                mainKG AS (
                    SELECT entity1 , relation, entity2, num FROM loginUser
                    UNION ALL
                    SELECT entity1 , relation, entity2, num FROM purchaseUser
                    UNION ALL
                    SELECT entity1 , relation, entity2, num FROM gtwpd.peiyuwu_kgrdfn_useriditemid_{eventStartDate.strftime('%Y%m%d')}
                )
                SELECT a.entity1, a.relation, a.entity2, a.num
                FROM mainKG a
                INNER JOIN loginUser b ON a.entity1 = b.entity1
                INNER JOIN onlyBuyFashionBoxUser c ON a.entity1 = c.entity1
            '''
        print(sql_str)
        df1 = hiveCtrl.searchSQL(sql_str)
        df1.to_csv(outputPath + f"/KGRDFN_maindf_{eventStartDate.strftime('%Y%m%d')}.csv")

        # event Graph
        sql_str = '''
                SELECT 
                    commondata_001, commondata_002, commondata_003, uniquefloat_001
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='P0_1_2'
                '''
        print(sql_str)
        df2 = hiveCtrl.searchSQL(sql_str)
        df2.columns = ['entity1', 'relation', 'entity2', 'num']
        df2.to_csv(outputPath + f"/KGRDFN_eventTagdf_{eventStartDate.strftime('%Y%m%d')}.csv")

        return "MakeRawDataFreeFuction", None, {}

    @classmethod
    def MakeRawData_KnowledgeGraph_R3_1_4(self, makeInfo):
        return "MakeRawDataFreeFuction", None

        hiveCtrl = RawPreModel.getHiveCtrl()
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_4/"

        for trainDate1_, trainDate2_, validDate_, eventDate_ in makeInfo['parameter']['makedate']:
            trainDate1 = datetime.datetime.strptime(trainDate1_, '%Y-%m-%d')
            trainDate2 = datetime.datetime.strptime(trainDate2_, '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_, '%Y-%m-%d')
            eventDate = datetime.datetime.strptime(eventDate_, '%Y-%m-%d')
            print(trainDate1, trainDate2, validDate, eventDate)

            # DataFrame
            sql_str = f''' 
                    WITH 
                    loginUser AS (
                        SELECT DISTINCT aa.service_account_id as service_account_id
                        FROM (
                            SELECT a.Entity1 as service_account_id
                            FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{trainDate1.strftime('%Y%m%d')} a
                            where a.num > 0
                            UNION ALL
                            SELECT b.Entity1 as service_account_id
                            FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{trainDate2.strftime('%Y%m%d')} b
                            where b.num > 0
                        ) aa
                    ),
                    onlyBuyFashionBoxUser AS (
                        SELECT DISTINCT Entity1 as service_account_id
                        FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{trainDate2.strftime('%Y%m%d')}
                    ),
                    purchase_box_train AS (
                        SELECT DISTINCT
                            CommonData_1 as service_account_id,
                            1 as num
                        FROM gtwpd.modelextract_modelextract AA
                        WHERE 1 = 1
                            AND AA.game = 'maple'
                            AND AA.tablenumber = '16009'
                            AND AA.dt >= {validDate.strftime('%Y%m%d')}
                            AND AA.dt < {eventDate.strftime('%Y%m%d')}
                            AND AA.UniqueStr_1 = 'bf point'
                            AND AA.UniqueStr_11 != 'RollBack'
                            AND AA.commondata_10 in ('5222123', '5680946')                  
                    )
                    SELECT 
                        a.service_account_id, c.num
                    FROM loginUser a
                    INNER JOIN onlyBuyFashionBoxUser b ON a.service_account_id = b.service_account_id
                    LEFT JOIN purchase_box_train c ON a.service_account_id = c.service_account_id
                '''
            print(sql_str)
            df = hiveCtrl.searchSQL(sql_str)
            df.columns = ['service_account_id', 'Y']
            df['data_set'] = validDate
            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                df['Y_item'] = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                df['Y_item'] = f'5680946_{validDate.strftime("%Y-%m-%d")}'
            df.fillna(0).to_csv(output_path + f"traindf_{validDate.strftime('%Y%m%d')}.csv")

        return "MakeRawDataFreeFuction", None

    @classmethod
    def MakeRawData_KnowledgeGraph_R3_1_5(self, makeInfo):
        return "MakeRawDataFreeFuction", None, {}

        hiveCtrl = RawPreModel.getHiveCtrl()
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][0]}/"

        for validDate_, eventDate_ in makeInfo['parameter']['makedate']:
            validDate = datetime.datetime.strptime(validDate_, '%Y-%m-%d')
            eventDate = datetime.datetime.strptime(eventDate_, '%Y-%m-%d')
            # print(trainDate1, trainDate2, validDate, eventDate)
            while True :
                try:
                    # DataFrame
                    sql_str = f''' 
                        WITH 
                        loginUser AS (
                            SELECT DISTINCT aa.Entity1 as service_account_id
                            FROM (
                               SELECT
                                   commondata_1 as Entity1
                                   , 'Login' as Relation
                                   , 'maple' as Entity2
                                   , sum(
                                       cast((unix_timestamp(UniqueTime_2, 'yyyy-mm-dd HH:mm:ss') - unix_timestamp(UniqueTime_1, 'yyyy-mm-dd HH:mm:ss')	) / 3600 as int)
                                   ) as num
                               FROM
                                   gtwpd.modelextract_modelextract
                               WHERE 1=1
                               AND game='maple'
                                   AND dt >= {(validDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                                   AND dt < {validDate.strftime('%Y%m%d')}
                                   AND tablenumber = '1103'
                               GROUP BY commondata_1
                            ) aa
                            WHERE 1=1
                                AND aa.num >= 4
                        ),
                        true_account AS (
                            SELECT DISTINCT
                                aaa.commondata_1 as service_account_id
                            FROM (
                                SELECT 
                                    aa.commondata_1, 
                                    aa.uniquestr_1
                                FROM gtwpd.modelextract_modelextract aa
                                WHERE 1=1
                                    AND aa.game='maple'
                                    AND aa.dt >= {(validDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                                    AND aa.dt < {validDate.strftime('%Y%m%d')}
                                    AND aa.tablenumber = '1806'
                            ) aaa
                            INNER JOIN(
                                SELECT
                                    bb.uniquestr_1
                                    , COUNT(DISTINCT bb.commondata_1) as account_num
                                FROM gtwpd.modelextract_modelextract bb
                                WHERE 1=1
                                    AND bb.game='maple'
                                    AND bb.dt >= {(validDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                                    AND bb.dt < {validDate.strftime('%Y%m%d')}
                                    AND bb.tablenumber = '1806'
                                GROUP BY bb.uniquestr_1
                            ) bbb on aaa.uniquestr_1 = bbb.uniquestr_1 and bbb.account_num < 9 
                        ),
                        level_filter AS (
                            SELECT DISTINCT
                                aaa.commondata_1 as service_account_id
                                , bbb.Lv_max as Lv_max
                            FROM (
                                SELECT DISTINCT 
                                    aa.commondata_1
                                FROM gtwpd.modelextract_modelextract aa
                                WHERE 1=1
                                    AND aa.game='maple'
                                    AND aa.dt >= {(validDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                                    AND aa.dt < {validDate.strftime('%Y%m%d')}
                                    AND aa.tablenumber = '2002'
                            ) aaa
                            INNER JOIN(
                                SELECT
                                    bb.commondata_1
                                    , MAX(bb.uniqueint_5) as Lv_max
                                FROM gtwpd.modelextract_modelextract bb
                                WHERE 1=1
                                    AND bb.game='maple'
                                    AND bb.dt >= {(validDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                                    AND bb.dt < {validDate.strftime('%Y%m%d')}
                                    AND bb.tablenumber = '2002'
                                GROUP BY bb.commondata_1
                            ) bbb on aaa.commondata_1 = bbb.commondata_1 and bbb.Lv_max >= 220
                        ),
                        onlyBuyFashionBoxUser AS (
                            SELECT DISTINCT Entity1 as service_account_id
                            FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{validDate.strftime('%Y%m%d')}
                        ),
                        questTag AS (
                            SELECT 
                                BB.commondata_001
                                , BB.UniqueFloat_001 
                                , BB.UniqueFloat_002 
                                , BB.UniqueFloat_003 
                                , BB.UniqueFloat_004 
                                , BB.UniqueFloat_005 
                                , BB.UniqueFloat_006 
                                , BB.UniqueFloat_007 
                                , BB.UniqueFloat_008 
                                , BB.UniqueFloat_009 
                            FROM gtwpd.model_usedata BB 
                            where 1 = 1 
                                AND BB.product = 'maple'
                                AND BB.project = 'AutoTag'
                                AND BB.step = 'PreProcess'
                                AND BB.version = 'P0_2_2'
                                AND BB.dt = {validDate.strftime('%Y%m%d')}
                        ),
                        purchase_box_train AS (
                            SELECT DISTINCT
                                CommonData_1 as service_account_id,
                                1 as num
                            FROM gtwpd.modelextract_modelextract AA
                            WHERE 1 = 1
                                AND AA.game = 'maple'
                                AND AA.tablenumber = '16009'
                                AND AA.dt >= {validDate.strftime('%Y%m%d')}
                                AND AA.dt < {eventDate.strftime('%Y%m%d')}
                                AND AA.UniqueStr_1 = 'bf point'
                                AND AA.UniqueStr_11 != 'RollBack'
                                AND AA.commondata_10 in ('5222123', '5680946')                  
                        )
                        SELECT 
                            a.service_account_id, e.num, d.Lv_max
                            , [:tagStandard]
                        FROM loginUser a
                        INNER JOIN onlyBuyFashionBoxUser b ON a.service_account_id = b.service_account_id
                        INNER JOIN true_account c ON a.service_account_id = c.service_account_id
                        INNER JOIN level_filter d ON a.service_account_id = d.service_account_id
                        LEFT JOIN purchase_box_train e ON a.service_account_id = e.service_account_id
                        LEFT JOIN questTag f ON a.service_account_id = f.commondata_001
                    '''
                    tag_cols = []
                    for i_ in range(9):
                        sql_str = sql_str.replace("[:tagStandard]",
                                                  f"f.UniqueFloat_{str(i_+1).zfill(3)} \n\t\t\t\t\t\t\t, [:tagStandard]")
                        tag_cols.append(f'tag{str(i_).zfill(3)}')

                    sql_str = sql_str.replace("\n\t\t\t\t\t\t\t, [:tagStandard]", f"")
                    print(sql_str)
                    df = hiveCtrl.searchSQL(sql_str)
                    df.columns = ['service_account_id', 'Y', 'LevelMax'] + tag_cols
                    df['data_set'] = validDate
                    if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                        df['Y_item'] = f'5222123_{validDate.strftime("%Y-%m-%d")}'
                    else:
                        df['Y_item'] = f'5680946_{validDate.strftime("%Y-%m-%d")}'
                    df.fillna(0).to_csv(output_path + f"traindf_{validDate.strftime('%Y%m%d')}.csv")

                    break
                except :
                    print(1)
                    continue
        return "MakeRawDataFreeFuction", None, {'message':None}

    @classmethod
    def MakeRawData_KnowledgeGraph_R4_1_1(self, makeInfo):
        return "MakeRawDataFreeFuction", None, {}

    @classmethod
    def MakeRawData_KnowledgeGraph_R99_99_99(self, makeInfo):
        return "MakeRawDataFreeFuction", None,{}