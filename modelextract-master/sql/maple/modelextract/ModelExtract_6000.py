import time
from sql.common.modelextract.ModelExtract_6000 import ModelExtract_6000 as ModelExtract_6000_Common

class ModelExtract_6000(ModelExtract_6000_Common) :

    @classmethod
    def insert6019DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6019')
            SELECT
                BB.CommonData_1 as CommonData_1
                , BB.CommonData_2  as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , BB.CommonData_4 as CommonData_4
                , BB.CommonData_5 as CommonData_5
                , BB.CommonData_6 as CommonData_6
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , AA.CommonData_8 as CommonData_8
                , CASE WHEN AA.CommonData_9 is not null THEN AA.CommonData_9 ELSE AA.CommonData_8 END as CommonData_9
                , AA.CommonData_10 as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , AA.UniqueInt_2 as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.UniqueInt_11 as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW bf point' ELSE 'HK bf point' END as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , AA.uniquestr_11 as uniquestr_11 
                , null as uniquestr_12 
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.uniquedbl_1 as uniquedbl_1 
                , AA.uniquedbl_2 as uniquedbl_2 
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
                , AA.UniqueTime_1 as UniqueTime_1
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
            INNER JOIN gtwpd.modelextract_modelextract BB ON 1 = 1
                AND BB.game = '[:GameName]'
                AND BB.dt = '[:DateNoLine]'
                AND BB.world = '[:World]'
                AND BB.tablenumber = 11003
                AND AA.CommonData_3 = BB.CommonData_3
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]' 
                AND AA.tablenumber = 16009 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]
    @classmethod
    def insert6301DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6301')
            SELECT
                BB.CommonData_1 as CommonData_1
                , BB.CommonData_2  as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , BB.CommonData_5 as CommonData_5
                , BB.CommonData_6 as CommonData_6
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , AA.CommonData_8 as CommonData_8
                , CASE WHEN AA.CommonData_9 is not null THEN AA.CommonData_9 ELSE AA.CommonData_8 END as CommonData_9
                , AA.CommonData_10 as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , AA.UniqueInt_2 as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.UniqueInt_11 as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW bf point' ELSE 'HK bf point' END as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , AA.uniquestr_11 as uniquestr_11 
                , null as uniquestr_12 
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.uniquedbl_1 as uniquedbl_1 
                , AA.uniquedbl_2 as uniquedbl_2 
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
                , AA.UniqueTime_1 as UniqueTime_1
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
            INNER JOIN gtwpd.modelextract_modelextract BB ON 1 = 1
                AND BB.game = '[:GameName]'
                AND BB.dt = '[:DateNoLine]'
                AND BB.tablenumber = 11002
                AND AA.CommonData_1 = BB.CommonData_1
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]' 
                AND AA.tablenumber = 16301 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]
    @classmethod
    def insert6401DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6401')
            SELECT
                BB.CommonData_1 as CommonData_1
                , BB.CommonData_2  as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , BB.CommonData_5 as CommonData_5
                , BB.CommonData_6 as CommonData_6
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , AA.CommonData_8 as CommonData_8
                , CASE WHEN AA.CommonData_9 is not null THEN AA.CommonData_9 ELSE AA.CommonData_8 END as CommonData_9
                , AA.CommonData_10 as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , AA.UniqueInt_2 as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.UniqueInt_11 as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW bf point' ELSE 'HK bf point' END as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , AA.uniquestr_11 as uniquestr_11 
                , null as uniquestr_12 
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.uniquedbl_1 as uniquedbl_1 
                , AA.uniquedbl_2 as uniquedbl_2 
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
                , AA.UniqueTime_1 as UniqueTime_1
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
            INNER JOIN gtwpd.modelextract_modelextract BB ON 1 = 1
                AND BB.game = '[:GameName]'
                AND BB.dt = '[:DateNoLine]'
                AND BB.tablenumber = 11002
                AND AA.CommonData_1 = BB.CommonData_1
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]' 
                AND AA.tablenumber = 16401 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]
    @classmethod
    def insert6509DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 6509 交易所(買)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6509')
            SELECT
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2  as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.CommonData_8 as CommonData_8
                , AA.CommonData_9 as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                -------------
                , AA.UniqueInt_3 as UniqueInt_1
                , 1 as UniqueInt_2
                , null as UniqueInt_3
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
                -------------
                , null as UniqueStr_1
                , AA.UniqueStr_2 as UniqueStr_2
                , AA.UniqueStr_3 as UniqueStr_3
                , AA.UniqueStr_4 as UniqueStr_4
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
                -------------
                , null as uniquedbl_1
                , null as uniquedbl_2
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
                -------------
                , AA.UniqueTime_1 as UniqueTime_1
                , AA.UniqueTime_2 as UniqueTime_2
                , null as UniqueTime_3
                -------------
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
                -------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                and AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '16509' ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6609DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 6609 一對一(收)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6609')
            SELECT
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2  as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.CommonData_8 as CommonData_8
                , AA.CommonData_9 as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                -------------
                , null as UniqueInt_1
                , 1 as UniqueInt_2
                , null as UniqueInt_3
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
                -------------
                , null as UniqueStr_1
                , null as UniqueStr_2
                , AA.UniqueStr_3 as UniqueStr_3
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
                -------------
                , null as uniquedbl_1
                , null as uniquedbl_2
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
                -------------
                , AA.UniqueTime_1 as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                -------------
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
                -------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                and AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '16609' ;
        """
        return "OrderInsert", [orderInsertSQLCode1]