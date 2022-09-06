import os
import pandas as pd
import numpy as np
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl



class RawData_KnowledgeGraph_OLD() :

    @classmethod
    def fashion_revised_all(self, path):
        read_equ = pd.read_csv(path)
        return read_equ

    @classmethod
    def MakeRawData_KnowledgeGraph_R0_0_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None
        """
        MakeRawDataFileInsert 方法說明
            會撈取 return rawDataType , rawDataObject 的 rawDataObject 裡面放置DF資料
            就會將 rawDataObject 裡的DF上傳至HDFS上面，並且利用Hive建立與HDFS的Partition

            上傳路徑為 /user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/version=[:Version]/dt=[:DateNoLine]/step=[:Step]
            像是該Function 就會上傳到/user/GTW_PD/DB/Model/Usedata/product=ExampleProduct/project=CheckProject/step=RawData/version=R1_0_3/dt=20211231
        """
        columns = [
            "commondata_001", "commondata_002", "commondata_003", "commondata_004", "commondata_005"
            , "commondata_006", "commondata_007", "commondata_008", "commondata_009", "commondata_010"
            , "commondata_011", "commondata_012", "commondata_013", "commondata_014", "commondata_015"
            , "uniquefloat_001", "uniquefloat_002", "uniquefloat_003", "uniquefloat_004", "uniquefloat_005"
            , "uniquefloat_006", "uniquefloat_007", "uniquefloat_008", "uniquefloat_009", "uniquefloat_010"
            , "uniquefloat_011", "uniquefloat_012", "uniquefloat_013", "uniquefloat_014", "uniquefloat_015"
            , "uniquefloat_016", "uniquefloat_017", "uniquefloat_018", "uniquefloat_019", "uniquefloat_020"
            , "uniquefloat_021", "uniquefloat_022", "uniquefloat_023", "uniquefloat_024", "uniquefloat_025"
            , "uniquefloat_026", "uniquefloat_027", "uniquefloat_028", "uniquefloat_029", "uniquefloat_030"
            , "uniquefloat_031", "uniquefloat_032", "uniquefloat_033", "uniquefloat_034", "uniquefloat_035"
            , "uniquefloat_036", "uniquefloat_037", "uniquefloat_038", "uniquefloat_039", "uniquefloat_040"
            , "uniquefloat_041", "uniquefloat_042", "uniquefloat_043", "uniquefloat_044", "uniquefloat_045"
            , "uniquefloat_046", "uniquefloat_047", "uniquefloat_048", "uniquefloat_049", "uniquefloat_050"
            , "uniquefloat_051", "uniquefloat_052", "uniquefloat_053", "uniquefloat_054", "uniquefloat_055"
            , "uniquefloat_056", "uniquefloat_057", "uniquefloat_058", "uniquefloat_059", "uniquefloat_060"
            , "uniquefloat_061", "uniquefloat_062", "uniquefloat_063", "uniquefloat_064", "uniquefloat_065"
            , "uniquefloat_066", "uniquefloat_067", "uniquefloat_068", "uniquefloat_069", "uniquefloat_070"
            , "uniquefloat_071", "uniquefloat_072", "uniquefloat_073", "uniquefloat_074", "uniquefloat_075"
            , "uniquefloat_076", "uniquefloat_077", "uniquefloat_078", "uniquefloat_079", "uniquefloat_080"
            , "uniquefloat_081", "uniquefloat_082", "uniquefloat_083", "uniquefloat_084", "uniquefloat_085"
            , "uniquefloat_086", "uniquefloat_087", "uniquefloat_088", "uniquefloat_089", "uniquefloat_090"
            , "uniquefloat_091", "uniquefloat_092", "uniquefloat_093", "uniquefloat_094", "uniquefloat_095"
            , "uniquefloat_096", "uniquefloat_097", "uniquefloat_098", "uniquefloat_099", "uniquefloat_100"
            , "uniquefloat_101", "uniquefloat_102", "uniquefloat_103", "uniquefloat_104", "uniquefloat_105"
            , "uniquefloat_106", "uniquefloat_107", "uniquefloat_108", "uniquefloat_109", "uniquefloat_110"
            , "uniquefloat_111", "uniquefloat_112", "uniquefloat_113", "uniquefloat_114", "uniquefloat_115"
            , "uniquefloat_116", "uniquefloat_117", "uniquefloat_118", "uniquefloat_119", "uniquefloat_120"
            , "uniquefloat_121", "uniquefloat_122", "uniquefloat_123", "uniquefloat_124", "uniquefloat_125"
            , "uniquefloat_126", "uniquefloat_127", "uniquefloat_128", "uniquefloat_129", "uniquefloat_130"
            , "uniquefloat_131", "uniquefloat_132", "uniquefloat_133", "uniquefloat_134", "uniquefloat_135"
            , "uniquefloat_136", "uniquefloat_137", "uniquefloat_138", "uniquefloat_139", "uniquefloat_140"
            , "uniquefloat_141", "uniquefloat_142", "uniquefloat_143", "uniquefloat_144", "uniquefloat_145"
            , "uniquefloat_146", "uniquefloat_147", "uniquefloat_148", "uniquefloat_149", "uniquefloat_150"
            , "uniquefloat_151", "uniquefloat_152", "uniquefloat_153", "uniquefloat_154", "uniquefloat_155"
            , "uniquefloat_156", "uniquefloat_157", "uniquefloat_158", "uniquefloat_159", "uniquefloat_160"
            , "uniquefloat_161", "uniquefloat_162", "uniquefloat_163", "uniquefloat_164", "uniquefloat_165"
            , "uniquefloat_166", "uniquefloat_167", "uniquefloat_168", "uniquefloat_169", "uniquefloat_170"
            , "uniquefloat_171", "uniquefloat_172", "uniquefloat_173", "uniquefloat_174", "uniquefloat_175"
            , "uniquefloat_176", "uniquefloat_177", "uniquefloat_178", "uniquefloat_179", "uniquefloat_180"
            , "uniquefloat_181", "uniquefloat_182", "uniquefloat_183", "uniquefloat_184", "uniquefloat_185"
            , "uniquefloat_186", "uniquefloat_187", "uniquefloat_188", "uniquefloat_189", "uniquefloat_190"
            , "uniquefloat_191", "uniquefloat_192", "uniquefloat_193", "uniquefloat_194", "uniquefloat_195"
            , "uniquefloat_196", "uniquefloat_197", "uniquefloat_198", "uniquefloat_199", "uniquefloat_200"
            , "uniquejson_001"]
        upload_df = pd.DataFrame([], columns=columns)
        print(upload_df)

        read_equ = self.read_equipment_revised_ALL(Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        read_equ['Date1'] = ''
        read_equ['Date2'] = ''
        dt_list = read_equ.Date.unique()
        for _ in range(len(dt_list)-1):
            read_equ.loc[read_equ.Date == dt_list[_], "Date1"] = datetime.datetime.strptime(dt_list[_], '%Y-%m-%d')
            read_equ.loc[read_equ.Date == dt_list[_], "Date2"] = datetime.datetime.strptime(dt_list[_+1], '%Y-%m-%d')# + datetime.timedelta(days=-1)
        read_equ = read_equ.drop(['Date'], axis=1)
        print(read_equ)

        upload_df['commondata_007'] = 'v238'
        upload_df['commondata_008'] = read_equ['ItemID']
        upload_df['commondata_009'] = read_equ['Name']
        upload_df['commondata_010'] = read_equ['Date1']
        upload_df['commondata_011'] = read_equ['Date2']
        upload_df['commondata_012'] = read_equ['KR Name']
        upload_df['commondata_013'] = read_equ['Part']
        upload_df['uniquefloat_001'] = read_equ['大師標籤']
        upload_df['uniquefloat_002'] = read_equ['IP合作']
        upload_df['uniquefloat_003'] = read_equ['漂浮特效']
        upload_df['uniquefloat_004'] = read_equ['暗色系']
        upload_df['uniquefloat_005'] = read_equ['螢光系']
        upload_df['uniquefloat_006'] = read_equ['柔和色系']
        upload_df['uniquefloat_007'] = read_equ['可愛風格']
        upload_df['uniquefloat_008'] = read_equ['制服風格']
        upload_df['uniquefloat_009'] = read_equ['貴族風格']
        upload_df['uniquefloat_010'] = read_equ['動物風格']
        upload_df['uniquefloat_011'] = read_equ['怪物風格']
        upload_df['uniquefloat_012'] = read_equ['角色風格']
        upload_df['uniquefloat_013'] = read_equ['自然風格']
        upload_df['uniquefloat_014'] = read_equ['重裝風格']
        upload_df['uniquefloat_015'] = read_equ['運動風格']
        upload_df['uniquefloat_016'] = read_equ['搞怪風格']
        upload_df['uniquefloat_017'] = read_equ['不擋身/腿']
        upload_df['uniquefloat_018'] = read_equ['不擋住名字']
        upload_df['uniquefloat_019'] = read_equ['渲染特效']
        upload_df['uniquefloat_020'] = read_equ['Prob.']
        upload_df['uniquefloat_021'] = read_equ['市價']


        print(upload_df.head(1))
        return "MakeRawDataFileInsert", upload_df

    @classmethod
    def MakeRawData_KnowledgeGraph_R1_0_1(self, makeInfo):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        # return "MakeRawDataFreeFuction", None

        read_equ = self.read_equipment_revised_ALL(Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        dt_list = read_equ.Date.unique()
        print(read_equ)
        # return "MakeRawDataFreeFuction", None


        # # 建userID購買fashionbox資料
        # dt_list = read_equ.Date.unique()
        # st_dt = datetime.datetime.strptime(dt_list[0], "%Y-%m-%d")
        # ed_dt = datetime.datetime.strptime(dt_list[-1], "%Y-%m-%d")
        # hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.{ed_dt.strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox")
        # sql_str = f'''
        #     CREATE TABLE IF NOT EXISTS gtwpd.{ed_dt.strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox AS
        #     SELECT
        #         AA.dt
        #         , AA.CommonData_1 as service_account_id
        #         , AA.CommonData_10 as item_id
        #     FROM gtwpd.modelextract_modelextract AA
        #     WHERE 1 = 1
        #         AND AA.game = 'maple'
        #         AND AA.tablenumber = '16009'
        #         AND AA.dt >= {st_dt.strftime('%Y%m%d')}
        #         AND AA.dt < {ed_dt.strftime('%Y%m%d')}
        #         AND AA.UniqueStr_1 = 'bf point'
        #         AND AA.UniqueStr_11 != 'RollBack'
        #         AND AA.commondata_10 in ('5222123', '5680946')
        # '''
        # print(sql_str)
        # hiveCtrl.executeSQL(sql_str)

        # 檔期間有登入的人
        dt_list = read_equ.Date.unique()
        for _ in range(36, len(dt_list)):
            st_dt = datetime.datetime.strptime(dt_list[_-1], "%Y-%m-%d")
            ed_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            print(st_dt)

            # 建userID登入資料
            hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdLogin")
            sql_str = f'''
                CREATE TABLE IF NOT EXISTS gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdLogin AS
                SELECT DISTINCT
                    commondata_1 as service_account_id
                FROM
                    gtwpd.modelextract_modelextract
                 WHERE 1=1
                 AND game='maple'
                    AND dt >=  {st_dt.strftime('%Y%m%d')}
                    AND dt < {ed_dt.strftime('%Y%m%d')}
                    AND tablenumber = '1103'
            '''
            print(sql_str)
            hiveCtrl.executeSQL(sql_str)
            # break

            # # 建userID持有資料(by event)
            # hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdItemId")
            # sql_str = f'''
            #     CREATE TABLE IF NOT EXISTS gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdItemId AS
            #     SELECT
            #         aa.commondata_1 as service_account_id
            #         , CONCAT_WS(',', COLLECT_SET(aa.commondata_8)) as item_set
            #     FROM
            #         gtwpd.modelextract_modelextract aa
            #     WHERE 1=1
            #         AND aa.game='maple'
            #         AND aa.dt >= {st_dt.strftime('%Y%m%d')}
            #         AND aa.dt < {ed_dt.strftime('%Y%m%d')}
            #         AND aa.tablenumber = '3012'
            #         AND aa.UniqueStr_1 = 'casheqp'
            #     GROUP BY aa.commondata_1
            # '''
            # print(sql_str)
            # hiveCtrl.executeSQL(sql_str)
            #
            # # 建userID購買資料(by event)
            # hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdPurchase")
            # sql_str = f'''
            #     CREATE TABLE IF NOT EXISTS gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdPurchase AS
            #     SELECT
            #         CommonData_1 as service_account_id
            #         , AA.CommonData_10 as item_id
            #         , COUNT(AA.CommonData_10) as item_id_num
            #     FROM gtwpd.modelextract_modelextract AA
            #     WHERE 1 = 1
            #         AND AA.game = 'maple'
            #         AND AA.tablenumber = '16009'
            #         AND AA.dt >= {st_dt.strftime('%Y%m%d')}
            #         AND AA.dt < {ed_dt.strftime('%Y%m%d')}
            #         AND AA.UniqueStr_1 = 'bf point'
            #         AND AA.UniqueStr_11 != 'RollBack'
            #         AND AA.commondata_10 IS NOT NULL
            #         AND AA.commondata_10 != '0'
            #     GROUP BY AA.CommonData_1, AA.CommonData_10
            # '''
            # print(sql_str)
            # hiveCtrl.executeSQL(sql_str)


        return "MakeRawDataFreeFuction", None

    @classmethod
    def MakeRawData_KnowledgeGraph_R2_0_1(self, makeInfo):
        print('Do MakeRawData_KnowledgeGraph_R2_0_1 ...')
        return "MakeRawDataFreeFuction", None

        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        ################################################################################################################
        read_equ = self.read_equipment_revised_ALL(Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        dt_list = read_equ.Date.unique()
        print(dt_list)

        for _ in range(36, len(dt_list)-1):
            st_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            print(f'event:{st_dt}')

            # 建userID持有資料(shift 3個月)
            sql_str = f''' SELECT * FROM gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdItemId'''
            print(sql_str)
            df = hiveCtrl.searchSQL(sql_str)
            cols = []
            for col_ in df.columns: cols.append(col_.split('.')[1])
            df.columns = cols
            df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_0_1/{st_dt.strftime('%Y%m%d')}_kgUserIdItemId.csv")

            # 建userID購買資料(shift 3個月)
            sql_str = f''' SELECT * FROM gtwpd.{st_dt.strftime('%Y%m%d')}_kguseridpurchase'''
            print(sql_str)
            df = hiveCtrl.searchSQL(sql_str)
            cols = []
            for col_ in df.columns: cols.append(col_.split('.')[1])
            df.columns = cols
            df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_0_1/{st_dt.strftime('%Y%m%d')}_kguseridpurchase.csv")

            # 建userID 登入資料
            sql_str = f''' SELECT * FROM gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdLogin'''
            print(sql_str)
            df = hiveCtrl.searchSQL(sql_str)
            cols = []
            for col_ in df.columns: cols.append(col_.split('.')[1])
            df.columns = cols
            df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_0_1/{st_dt.strftime('%Y%m%d')}_kgUserIdLogin.csv")


        # dt_list = read_equ.Date.unique()
        # st_dt = datetime.datetime.strptime(dt_list[-1], "%Y-%m-%d")
        #
        # # 建userID購買fashionbox資料
        # sql_str = f''' SELECT * FROM gtwpd.{st_dt.strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox'''
        # print(sql_str)
        # df = hiveCtrl.searchSQL(sql_str)
        # cols = []
        # for col_ in df.columns: cols.append(col_.split('.')[1])
        # df.columns = cols
        # df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_0_1/{st_dt.strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox.csv")
        #
        # # 檔期間有登入的人

        return "MakeRawDataFreeFuction", None

    @classmethod
    def MakeRawData_KnowledgeGraph_R3_0_1(self, makeInfo):
        return "MakeRawDataFreeFuction", None

    # 拼湊圖譜資料
    @classmethod
    def MakeRawData_KnowledgeGraph_R2_1_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None
        hiveCtrl = self.getHiveCtrl()
        # dt_list = self.getEventList(hiveCtrl)
        #
        # validDate_list = makeInfo['parameter']['rawdata']['makedate']
        # for ind_ in range(1, len(validDate_list)):
        #     eventStartDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
        #     eventEndDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
        #     print(eventStartDate, eventEndDate)
        #
        #     # Buy Box Graph
        #     sql_str = f'''
        #                     WITH
        #                     loginUser AS (
        #                        SELECT
        #                            commondata_1 as Entity1
        #                            , 'Login' as Relation
        #                            , 'maple' as Entity2
        #                            , sum(
        #                                cast((unix_timestamp(UniqueTime_2, 'yyyy-mm-dd HH:mm:ss') - unix_timestamp(UniqueTime_1, 'yyyy-mm-dd HH:mm:ss')	) / 3600 as int)
        #                            ) as num
        #                        FROM
        #                            gtwpd.modelextract_modelextract
        #                        WHERE 1=1
        #                        AND game='maple'
        #                            AND dt >= {(eventEndDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
        #                            AND dt < {eventEndDate.strftime('%Y%m%d')}
        #                            AND tablenumber = '1103'
        #                        GROUP BY commondata_1
        #                     )
        #                     SELECT
        #                         a.entity1, a.relation, a.entity2, a.num, a.dt
        #                     FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')} a
        #                     INNER JOIN loginUser b ON a.entity1 = b.entity1
        #                 '''
        #     print(sql_str)
        #     kgUserIdPurchaseFashionBox = hiveCtrl.searchSQL(sql_str)
        #     kgUserIdPurchaseFashionBox.columns = ['entity1', 'relation', 'entity2', 'num', 'dt']
        #     print(kgUserIdPurchaseFashionBox.loc[kgUserIdPurchaseFashionBox.entity1 == '0007946123'])
        #     '''
        #                            entity1 relation  entity2  num        dt
        #                     151854  0007946123       購買  5680946    1  20210908
        #                     151856  0007946123       購買  5680946    1  20210908
        #                     151859  0007946123       購買  5680946    1  20210908
        #                     151862  0007946123       購買  5680946    1  20210908
        #                     151863  0007946123       購買  5680946    1  20210908
        #                     152884  0007946123       購買  5680946    1  20210908
        #                     152885  0007946123       購買  5680946    1  20210908
        #                     206526  0007946123       購買  5680946    1  20211215
        #                     206527  0007946123       購買  5680946    1  20211215
        #                     245683  0007946123       購買  5680946    1  20211016
        #                     245684  0007946123       購買  5680946    1  20211016
        #                     245685  0007946123       購買  5680946    1  20211016
        #                     245686  0007946123       購買  5680946    1  20211016
        #                     245687  0007946123       購買  5680946    1  20211016
        #                     245785  0007946123       購買  5680946    1  20211016
        #                     245787  0007946123       購買  5680946    1  20211016
        #                     245788  0007946123       購買  5680946    1  20211016
        #                     245789  0007946123       購買  5680946    1  20211016
        #                     245790  0007946123       購買  5680946    1  20211016
        #                     275572  0007946123       購買  5680946    1  20211125
        #                 '''
        #
        #     df = self.reorganizeKgUserIdPurchaseFashionBox(kgUserIdPurchaseFashionBox, dt_list)
        #     print(df.loc[df.Entity1 == '0007946123'])
        #     '''
        #           Entity1 Relation             Entity2  num          dt
        #     3  0007946123       購買  5680946_2021-09-08    7  2021-09-08
        #     4  0007946123       購買  5680946_2021-09-29   10  2021-09-29
        #     5  0007946123       購買  5680946_2021-11-17    1  2021-11-17
        #     6  0007946123       購買  5680946_2021-12-15    2  2021-12-15
        #     '''
        #     df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_eventBuydf_{eventStartDate.strftime('%Y%m%d')}.csv")
        #
        #     # main Graph
        #     sql_str = f'''
        #             WITH
        #             loginUser AS (
        #                SELECT
        #                    commondata_1 as Entity1
        #                    , 'Login' as Relation
        #                    , 'maple' as Entity2
        #                    , sum(
        #                        cast((unix_timestamp(UniqueTime_2, 'yyyy-mm-dd HH:mm:ss') - unix_timestamp(UniqueTime_1, 'yyyy-mm-dd HH:mm:ss')	) / 3600 as int)
        #                    ) as num
        #                FROM
        #                    gtwpd.modelextract_modelextract
        #                WHERE 1=1
        #                AND game='maple'
        #                    AND dt >= {(eventEndDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
        #                    AND dt < {eventEndDate.strftime('%Y%m%d')}
        #                    AND tablenumber = '1103'
        #                GROUP BY commondata_1
        #             ),
        #             purchaseUser AS (
        #                SELECT
        #                    CommonData_1 as Entity1
        #                    , '購買' as Relation
        #                    , AA.CommonData_10 as Entity2
        #                    , COUNT(AA.CommonData_10) as num
        #                FROM gtwpd.modelextract_modelextract AA
        #                WHERE 1 = 1
        #                    AND AA.game = 'maple'
        #                    AND AA.tablenumber = '16009'
        #                    AND AA.dt >= {(eventEndDate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
        #                    AND AA.dt < {eventEndDate.strftime('%Y%m%d')}
        #                    AND AA.UniqueStr_1 = 'bf point'
        #                    AND AA.UniqueStr_11 != 'RollBack'
        #                    AND AA.commondata_10 IS NOT NULL
        #                    AND AA.commondata_10 != '0'
        #                GROUP BY AA.CommonData_1, AA.CommonData_10
        #             ),
        #             onlyBuyFashionBoxUser AS (
        #                 SELECT DISTINCT Entity1
        #                 FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')}
        #             ),
        #             mainKG AS (
        #                 SELECT entity1 , relation, entity2, num FROM loginUser
        #                 UNION ALL
        #                 SELECT entity1 , relation, entity2, num FROM purchaseUser
        #                 UNION ALL
        #                 SELECT entity1 , relation, entity2, num FROM gtwpd.peiyuwu_kgrdfn_useriditemid_{eventStartDate.strftime('%Y%m%d')}
        #             )
        #             SELECT a.entity1, a.relation, a.entity2, a.num
        #             FROM mainKG a
        #             INNER JOIN loginUser b ON a.entity1 = b.entity1
        #             INNER JOIN onlyBuyFashionBoxUser c ON a.entity1 = c.entity1
        #         '''
        #     print(sql_str)
        #     df1 = hiveCtrl.searchSQL(sql_str)
        #     df1.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_maindf_{eventStartDate.strftime('%Y%m%d')}.csv")

        # event Graph
        sql_str = '''
                SELECT 
                    commondata_001, commondata_002, commondata_003, uniquefloat_001
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='P0_1_1'
                '''
        print(sql_str)
        df2 = hiveCtrl.searchSQL(sql_str)
        df2.columns = ['entity1', 'relation', 'entity2', 'num']

        validDate_list = makeInfo['parameter']['rawdata']['makedate']
        for ind_ in range(1, len(validDate_list)):
            eventStartDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            eventEndDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(eventStartDate, eventEndDate)
            df2.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_eventTagdf_{eventStartDate.strftime('%Y%m%d')}.csv")

        return "MakeRawDataFreeFuction", None


    @classmethod
    def MakeRawData_KnowledgeGraph_R2_1_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None
        hiveCtrl = self.getHiveCtrl()
        dt_list = self.getEventList(hiveCtrl)

        for makedate_ in makeInfo['parameter']['rawdata']['makedate']:
            eventStartDate, eventEndDate = self.getLastOverEventRange(makedate_, hiveCtrl)
            print(eventStartDate, eventEndDate)
            # getLast2OverEventRange
            # Buy Box Graph
            sql_str = f'''
                            WITH
                            loginUser AS (
                                SELECT DISTINCT Entity2
                                FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{eventStartDate.strftime('%Y%m%d')}
                                where num > 0
                            )
                            SELECT
                                a.entity1, a.relation, a.entity2, a.num, a.dt
                            FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')} a
                            INNER JOIN loginUser b ON a.entity1 = b.entity2
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

            df = self.reorganizeKgUserIdPurchaseFashionBox(kgUserIdPurchaseFashionBox, dt_list)
            print(df.loc[df.Entity1 == '0007946123'])
            '''
                  Entity1 Relation             Entity2  num          dt
            3  0007946123       購買  5680946_2021-09-08    7  2021-09-08
            4  0007946123       購買  5680946_2021-09-29   10  2021-09-29
            5  0007946123       購買  5680946_2021-11-17    1  2021-11-17
            6  0007946123       購買  5680946_2021-12-15    2  2021-12-15
            '''
            df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_eventBuydf_{eventStartDate.strftime('%Y%m%d')}.csv")

            # main Graph
            sql_str = f'''
                    WITH
                    loginUser AS (
                        SELECT DISTINCT Entity2
                        FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{eventStartDate.strftime('%Y%m%d')}
                        where num > 0
                    ),
                    onlyBuyFashionBoxUser AS (
                        SELECT DISTINCT Entity1
                        FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')}
                    ),
                    mainKG AS (
                        SELECT entity2 as kg_entity1 , relation as kg_relation, entity1 as kg_entity2 , num as kg_num FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{eventStartDate.strftime('%Y%m%d')}
                        UNION ALL
                        SELECT entity1 as kg_entity1 , relation as kg_relation, entity2 as kg_entity2 , num as kg_num FROM gtwpd.peiyuwu_kgrdfn_useridpurchase_{eventStartDate.strftime('%Y%m%d')}
                        UNION ALL
                        SELECT entity1 as kg_entity1 , relation as kg_relation, entity2 as kg_entity2 , num as kg_num FROM gtwpd.peiyuwu_kgrdfn_useriditemid_{eventStartDate.strftime('%Y%m%d')}
                    ),
                    main_tb as (
                        SELECT *
                        FROM mainKG a
                        INNER JOIN loginUser b ON a.kg_entity1 = b.entity2
                        INNER JOIN onlyBuyFashionBoxUser c ON a.kg_entity1 = c.entity1
                    )
                    SELECT
                        CASE WHEN kg_relation = 'Logined' THEN kg_entity2 ELSE kg_entity1 END AS Entity1
                        , kg_relation as Relation
                        , CASE WHEN kg_relation = 'Logined' THEN kg_entity1 ELSE kg_entity2 END AS Entity2
                        , kg_num as num
                    FROM main_tb
                '''
            print(sql_str)
            df1 = hiveCtrl.searchSQL(sql_str)
            df1.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_maindf_{eventStartDate.strftime('%Y%m%d')}.csv")

            # event Graph
            sql_str = '''
                    SELECT 
                        commondata_001, commondata_002, commondata_003, uniquefloat_001
                    FROM gtwpd.model_usedata  
                    WHERE 1=1
                        AND product = 'maple'
                        AND project='KnowledgeGraph'
                        AND version='P0_1_1'
                    '''
            print(sql_str)
            df2 = hiveCtrl.searchSQL(sql_str)
            df2.columns = ['entity1', 'relation', 'entity2', 'num']
            df2.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_eventTagdf_{eventStartDate.strftime('%Y%m%d')}.csv")
        return "MakeRawDataFreeFuction", None

    @classmethod
    def MakeRawData_KnowledgeGraph_R3_1_2(self, makeInfo):
        return "MakeRawDataFreeFuction", None

        hiveCtrl = self.getHiveCtrl()
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_2/"

        for makedate_ in makeInfo['parameter']['rawdata']['makedate']:
            eventDate1, eventDate2, eventDate3 = self.getTrainValidEventRange(makedate_, hiveCtrl)
            print(eventDate1, eventDate2, eventDate3)

            # DataFrame
            sql_str = f''' 
                    WITH 
                    loginUser AS (
                        SELECT DISTINCT Entity2 as service_account_id
                        FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{eventDate1.strftime('%Y%m%d')}
                        where num > 0
                    ),
                    onlyBuyFashionBoxUser AS (
                        SELECT DISTINCT Entity1 as service_account_id
                        FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventDate1.strftime('%Y%m%d')}
                    ),
                    purchase_box_train AS (
                        SELECT DISTINCT
                            CommonData_1 as service_account_id,
                            1 as num
                        FROM gtwpd.modelextract_modelextract AA
                        WHERE 1 = 1
                            AND AA.game = 'maple'
                            AND AA.tablenumber = '16009'
                            AND AA.dt >= {eventDate2.strftime('%Y%m%d')}
                            AND AA.dt < {eventDate3.strftime('%Y%m%d')}
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
            if eventDate2 < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                df['Y_item'] = f'5222123_{eventDate2.strftime("%Y-%m-%d")}'
            else:
                df['Y_item'] = f'5680946_{eventDate2.strftime("%Y-%m-%d")}'
            df.fillna(0).to_csv(output_path + f"traindf_{eventDate2.strftime('%Y%m%d')}.csv")
        return "MakeRawDataFreeFuction", None

    @classmethod
    def MakeRawData_KnowledgeGraph_R3_1_1(self, makeInfo):
        return "MakeRawDataFreeFuction", None

        hiveCtrl = RawPreModel.getHiveCtrl()
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_1/"

        for validDate_ in makeInfo['parameter']['rawdata']['validDate']:
            trainDate, validDate, eventDate = self.getTrainValidEventRange(validDate_, hiveCtrl)
            print(trainDate, validDate, eventDate)

            # DataFrame
            sql_str = f''' 
                    WITH 
                    loginUser AS (
                        SELECT DISTINCT Entity2 as service_account_id
                        FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{trainDate.strftime('%Y%m%d')}
                        where num > 0
                    ),
                    onlyBuyFashionBoxUser AS (
                        SELECT DISTINCT Entity1 as service_account_id
                        FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{trainDate.strftime('%Y%m%d')}
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
            df.fillna(0).to_csv(output_path + f"traindf_{trainDate.strftime('%Y%m%d')}.csv")

        return "MakeRawDataFreeFuction", None

    @classmethod
    def MakeRawData_KnowledgeGraph_R3_1_3(self, makeInfo):
        return "MakeRawDataFreeFuction", None

        hiveCtrl = RawPreModel.getHiveCtrl()
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_3/"

        validDate_list = makeInfo['parameter']['rawdata']['validDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(trainDate, validDate)

            # DataFrame
            sql_str = f''' 
                    WITH 
                    loginUser AS (
                        SELECT DISTINCT Entity1 as service_account_id
                        FROM gtwpd.peiyuwu_KGRDFN_userIdLogin_{trainDate.strftime('%Y%m%d')}
                        where num > 0
                    ),
                    onlyBuyFashionBoxUser AS (
                        SELECT DISTINCT Entity1 as service_account_id
                        FROM gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{trainDate.strftime('%Y%m%d')}
                    ),
                    purchase_box_train AS (
                        SELECT DISTINCT
                            CommonData_1 as service_account_id,
                            1 as num
                        FROM gtwpd.modelextract_modelextract AA
                        WHERE 1 = 1
                            AND AA.game = 'maple'
                            AND AA.tablenumber = '16009'
                            AND AA.dt >= {trainDate.strftime('%Y%m%d')}
                            AND AA.dt < {validDate.strftime('%Y%m%d')}
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
            df['data_set'] = trainDate
            if trainDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                df['Y_item'] = f'5222123_{trainDate.strftime("%Y-%m-%d")}'
            else:
                df['Y_item'] = f'5680946_{trainDate.strftime("%Y-%m-%d")}'
            df.fillna(0).to_csv(output_path + f"traindf_{trainDate.strftime('%Y%m%d')}.csv")

        return "MakeRawDataFreeFuction", None

