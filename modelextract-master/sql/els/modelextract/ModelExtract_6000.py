import time
from sql.common.modelextract.ModelExtract_6000 import ModelExtract_6000 as ModelExtract_6000_Common


class ModelExtract_6000(ModelExtract_6000_Common):

    @classmethod
    def insert6011DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelExtract 6011
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='6011')
            SELECT
                AA.CommonData_1
                , AA.CommonData_2
                , AA.CommonData_3
                , AA.CommonData_4
                , AA.CommonData_5
                , AA.CommonData_6
                , AA.CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , SUM(AA.UniqueInt_1) as UniqueInt_1
                , count(AA.UniqueInt_2) as UniqueInt_2
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
                , null as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as UniqueStr_11
                            , null as UniqueStr_12
                            , null as UniqueStr_13
                            , null as UniqueStr_14
                            , null as UniqueStr_15
                            , null as UniqueStr_16
                            , null as UniqueStr_17
                            , null as UniqueStr_18
                            , null as UniqueStr_19
                            , null as UniqueStr_20
                            , null as UniqueDBL_1
                            , null as UniqueDBL_2
                            , null as UniqueDBL_3
                            , null as UniqueDBL_4
                            , null as UniqueDBL_5
                            , null as UniqueDBL_6
                            , null as UniqueDBL_7
                            , null as UniqueDBL_8
                            , null as UniqueDBL_9
                            , null as UniqueDBL_10
                            , null as UniqueDBL_11
                            , null as UniqueDBL_12
                            , null as UniqueDBL_13
                            , null as UniqueDBL_14
                            , null as UniqueDBL_15
                            , null as UniqueDBL_16
                            , null as UniqueDBL_17
                            , null as UniqueDBL_18
                            , null as UniqueDBL_19
                            , null as UniqueDBL_20
                , null as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as OtherStr_1
                            , null as OtherStr_2
                            , null as OtherStr_3
                            , null as OtherStr_4
                            , null as OtherStr_5
                            , null as OtherStr_6
                            , null as OtherStr_7
                            , null as OtherStr_8
                            , null as OtherStr_9
                            , null as OtherStr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = 'COMMON'
                AND AA.tablenumber = '6019'
            GROUP BY
                AA.CommonData_1
                , AA.CommonData_2
                , AA.CommonData_3
                , AA.CommonData_4
                , AA.CommonData_5
                , AA.CommonData_6
                , AA.commondata_7;
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6012DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelExtract 6012
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='6012')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , AA.CommonData_7
                , AA.CommonData_8
                , AA.CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , SUM(AA.UniqueInt_1) as UniqueInt_1
                , SUM(AA.UniqueInt_2) as UniqueInt_2
                , AA.UniqueInt_3
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
                , 'bf point' as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as UniqueStr_11
                            , null as UniqueStr_12
                            , null as UniqueStr_13
                            , null as UniqueStr_14
                            , null as UniqueStr_15
                            , null as UniqueStr_16
                            , null as UniqueStr_17
                            , null as UniqueStr_18
                            , null as UniqueStr_19
                            , null as UniqueStr_20
                            , null as UniqueDBL_1
                            , null as UniqueDBL_2
                            , null as UniqueDBL_3
                            , null as UniqueDBL_4
                            , null as UniqueDBL_5
                            , null as UniqueDBL_6
                            , null as UniqueDBL_7
                            , null as UniqueDBL_8
                            , null as UniqueDBL_9
                            , null as UniqueDBL_10
                            , null as UniqueDBL_11
                            , null as UniqueDBL_12
                            , null as UniqueDBL_13
                            , null as UniqueDBL_14
                            , null as UniqueDBL_15
                            , null as UniqueDBL_16
                            , null as UniqueDBL_17
                            , null as UniqueDBL_18
                            , null as UniqueDBL_19
                            , null as UniqueDBL_20
                , null as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as OtherStr_1
                            , null as OtherStr_2
                            , null as OtherStr_3
                            , null as OtherStr_4
                            , null as OtherStr_5
                            , null as OtherStr_6
                            , null as OtherStr_7
                            , null as OtherStr_8
                            , null as OtherStr_9
                            , null as OtherStr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = 'COMMON'
                AND AA.tablenumber = '6019'
            GROUP BY
                AA.commondata_7
                , AA.CommonData_8
                , AA.CommonData_9
                , AA.UniqueInt_3 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6019DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
        -- Make [:GameName] ModelExtract 6019
        INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='6019')
        SELECT
               T1.accountid as CommonData_1
               , T2.useruid as CommonData_2
               , null as CommonData_3
               , null as CommonData_4
               , T3.mainaccountid as CommonData_5
               , T3.openid as CommonData_6
               , CASE WHEN T3.mainaccountid is not null then 'TW' else 'HK' end as CommonData_7
               , item_id as CommonData_8
               , case when T4.itemname is not null then T4.itemname else T1.memo end as CommonData_9
               , null as CommonData_10
               , null as CommonData_11
               , null as CommonData_12
               , null as CommonData_13
               , null as CommonData_14
               , null as CommonData_15
               , total_payment as UniqueInt_1
               , item_quantity as UniqueInt_2
               , item_price as UniqueInt_3
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
               , null as UniqueStr_1
               , null as UniqueStr_2
               , null as UniqueStr_3
               , null as UniqueStr_4
               , null as UniqueStr_5
               , null as UniqueStr_6
               , null as UniqueStr_7
               , null as UniqueStr_8
               , null as UniqueStr_9
               , null as UniqueStr_10
               , null as UniqueStr_11
               , null as UniqueStr_12
               , null as UniqueStr_13
               , null as UniqueStr_14
               , null as UniqueStr_15
               , null as UniqueStr_16
               , null as UniqueStr_17
               , null as UniqueStr_18
               , null as UniqueStr_19
               , null as UniqueStr_20
               , null as UniqueDBL_1
               , null as UniqueDBL_2
               , null as UniqueDBL_3
               , null as UniqueDBL_4
               , null as UniqueDBL_5
               , null as UniqueDBL_6
               , null as UniqueDBL_7
               , null as UniqueDBL_8
               , null as UniqueDBL_9
               , null as UniqueDBL_10
               , null as UniqueDBL_11
               , null as UniqueDBL_12
               , null as UniqueDBL_13
               , null as UniqueDBL_14
               , null as UniqueDBL_15
               , null as UniqueDBL_16
               , null as UniqueDBL_17
               , null as UniqueDBL_18
               , null as UniqueDBL_19
               , null as UniqueDBL_20
               , purchase_time as UniqueTime_1
               , null as UniqueTime_2
               , null as UniqueTime_3
               , null as OtherStr_1
               , null as OtherStr_2
               , null as OtherStr_3
               , null as OtherStr_4
               , null as OtherStr_5
               , null as OtherStr_6
               , null as OtherStr_7
               , null as OtherStr_8
               , null as OtherStr_9
               , null as OtherStr_10
               , array(null) as UniqueArray_01
               , array(null) as UniqueArray_02
               , null as UniqueJson_01
        FROM
            (SELECT DISTINCT
                lower(userid) as accountid,
                cast(itemid as int) as item_id,
                memo,
                1 as item_quantity,
                point as item_price,
                point as total_payment,
                createdate as purchase_time
             FROM els_all.convertcenter_elstranslog
             WHERE 1 = 1
             AND (
                ('[:DateNoLine]' < '20200901' AND dt = '20200901')
                OR ('[:DateNoLine]' >='20200901' AND dt = DATE_FORMAT(DATE_ADD('[:DateLine]', 1),'yyyyMMdd'))
                )
             AND createdate >= concat('[:DateLine]', ' 00:00:00') AND createdate < concat(DATE_ADD('[:DateLine]', 1), ' 00:00:00')
             AND createdate is not null
            ) T1
        LEFT OUTER JOIN
            (SELECT lower(useruid) as useruid,
                    lower(userid) as serviceid
             FROM els_all.account_muser
             WHERE 1 = 1
             AND dt = '[:DateNoLine]'
            ) T2
        ON T1.accountid = T2.serviceid
        LEFT OUTER JOIN
            (SELECT CommonData_1 as serviceaccountid,
                    CommonData_2 as mainaccountid,
                    CommonData_3 as openid
             FROM gtwpd.modelextract_modelextract
             WHERE 1 = 1
             AND dt = '[:DateNoLine]'
             AND tablenumber = '11001'
             AND game = 'mabi'
             AND world = 'COMMON'
            ) T3
        ON T1.accountid = T3.serviceaccountid
        LEFT OUTER JOIN
            (SELECT itemcode,
                    itemname
             FROM els_extract.game01_gitemcode
             WHERE 1 = 1
             AND dt = '[:DateNoLine]'
            ) T4
        ON T1.item_id = T4.itemcode;
        """
        return "OrderInsert", [orderInsertSQLCode1]

