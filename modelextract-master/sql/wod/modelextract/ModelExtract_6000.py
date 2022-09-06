import time
from sql.common.modelextract.ModelExtract_6000 import ModelExtract_6000 as ModelExtract_6000_Common

class ModelExtract_6000(ModelExtract_6000_Common) :

    @classmethod
    def insert6001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='6001')
            SELECT 
                AA.CommonData_1 AS CommonData_1 -- 帳號名稱
                , AA.CommonData_2 AS CommonData_2 -- 帳號名稱
                , null AS CommonData_3 
                , null AS CommonData_4 
                , null as CommonData_5
                , AA.CommonData_6 as CommonData_6 -- openID
                , null as CommonData_7
                , AA.CommonData_8 as CommonData_8 -- 道具ID
                , AA.CommonData_9 as CommonData_9 -- 道具名稱 假如無道具名稱則用ID顯示
                , AA.CommonData_10 as CommonData_10 -- 平台
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AA.UniqueInt_1 as UniqueInt_1 --道具總價
                , AA.UniqueInt_2 as UniqueInt_2 -- 道具數量
                , AA.UniqueInt_3 as UniqueInt_3 --道具單價
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11 
                , null as UniqueInt_12 
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , AA.UniqueStr_1 as UniqueStr_1 -- 貨幣類別
                , AA.UniqueStr_2 as UniqueStr_2 -- 付費平台
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as uniquestr_11 
                , null as uniquestr_12 
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.uniquedbl_1 as uniquedbl_1 --道具總價
                , AA.uniquedbl_2 as uniquedbl_2 --道具單價
                , null as uniquedbl_3 
                , null as uniquedbl_4 
                , null as uniquedbl_5 
                , null as uniquedbl_6 
                , null as uniquedbl_7 
                , null as uniquedbl_8 
                , null as uniquedbl_9 
                , null as uniquedbl_10 
                , null as uniquedbl_11 
                , null as uniquedbl_12 
                , null as uniquedbl_13 
                , null as uniquedbl_14 
                , null as uniquedbl_15 
                , null as uniquedbl_16 
                , null as uniquedbl_17 
                , null as uniquedbl_18 
                , null as uniquedbl_19 
                , null as uniquedbl_20
                , AA.UniqueTime_1 as UniqueTime_1 -- 消費時間
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1 
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1 
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.tablenumber = '16001' ; 
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='6002')
            SELECT 
                AA.CommonData_1 AS CommonData_1 -- 帳號ID
                , AA.CommonData_2 AS CommonData_2 -- 帳號ID
                , null AS CommonData_3
                , null AS CommonData_4
                , null as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , null as CommonData_7
                , AA.CommonData_8 as CommonData_8 -- 道具ID
                , AA.CommonData_9 as CommonData_9 -- 道具名稱 假如無道具名稱則用ID顯示
                , AA.CommonData_10 as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1 --道具總價
                , AA.UniqueInt_2 as UniqueInt_2 -- 道具數量
                , null as UniqueInt_3 --道具單價
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11 
                , null as UniqueInt_12_other 
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , AA.UniqueStr_1 as UniqueStr_1 -- 貨幣類型
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as uniquestr_11 
                , null as uniquestr_12 
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.uniquedbl_1 as uniquedbl_1 --貨幣消耗
                , AA.uniquedbl_2 as uniquedbl_2 --道具單價
                , null as uniquedbl_3 
                , null as uniquedbl_4 
                , null as uniquedbl_5 
                , null as uniquedbl_6 
                , null as uniquedbl_7 
                , null as uniquedbl_8 
                , null as uniquedbl_9 
                , null as uniquedbl_10 
                , null as uniquedbl_11 
                , null as uniquedbl_12 
                , null as uniquedbl_13 
                , null as uniquedbl_14 
                , null as uniquedbl_15 
                , null as uniquedbl_16 
                , null as uniquedbl_17 
                , null as uniquedbl_18 
                , null as uniquedbl_19 
                , null as uniquedbl_20
                , AA.UniqueTime_1 as UniqueTime_1 -- 資料時間
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1 -- 原始資料
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1 
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.tablenumber = '16002'
                AND AA.UniqueInt_12 = 2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6019DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6019')
            SELECT 
                AA.CommonData_1 AS CommonData_1 -- 帳號名稱
                , AA.CommonData_2 AS CommonData_2 -- 帳號名稱
                , AA.CommonData_3 AS CommonData_3 -- 角色ID
                , AA.CommonData_4 AS CommonData_4 -- 角色名稱
                , null as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , null as CommonData_7
                , AA.CommonData_8 as CommonData_8 -- 道具ID
                , AA.CommonData_9 as CommonData_9 -- 道具名稱 假如無道具名稱則用ID顯示
                , AA.CommonData_10 as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1 --道具總價
                , AA.UniqueInt_2 as UniqueInt_2 -- 道具數量
                , null as UniqueInt_3 --道具單價
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11 
                , null as UniqueInt_12_other 
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , AA.UniqueStr_1 as UniqueStr_1 -- 貨幣類型
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as uniquestr_11 
                , null as uniquestr_12
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.uniquedbl_1 as uniquedbl_1 --貨幣消耗
                , AA.uniquedbl_2 as uniquedbl_2 --道具單價
                , null as uniquedbl_3 
                , null as uniquedbl_4 
                , null as uniquedbl_5 
                , null as uniquedbl_6 
                , null as uniquedbl_7 
                , null as uniquedbl_8 
                , null as uniquedbl_9 
                , null as uniquedbl_10 
                , null as uniquedbl_11 
                , null as uniquedbl_12 
                , null as uniquedbl_13 
                , null as uniquedbl_14 
                , null as uniquedbl_15 
                , null as uniquedbl_16 
                , null as uniquedbl_17 
                , null as uniquedbl_18 
                , null as uniquedbl_19 
                , null as uniquedbl_20
                , AA.UniqueTime_1 as UniqueTime_1 -- 資料時間
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1 
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '16002'
                AND AA.UniqueInt_12 = 2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

