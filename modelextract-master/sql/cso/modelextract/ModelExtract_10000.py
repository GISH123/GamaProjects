import time


class ModelExtract_10000 :

    @classmethod
    def insert11001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 11001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11001')
            SELECT 
                T1.serviceaccountid as CommonData_1
                , T1.mainaccountid  as CommonData_2
                , T2.openid as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , null as UniqueInt_2
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
        FROM
            (SELECT
                lower(serviceaccountid) as serviceaccountid,
                MAX(lower(mainaccount)) as mainaccountid
             FROM bf_extract.beanfundb_d_serviceaccount_f
             WHERE 1 = 1
             AND servicecode = '610153'
             AND ('[:DateNoLine]' < '20200701' AND dt = '20200701')
             OR ('[:DateNoLine]' >='20200701' AND dt >= '[:DateNoLine]' AND dt <= DATE_FORMAT(DATE_ADD('[:DateLine]', 2),'yyyyMMdd'))
             GROUP BY lower(serviceaccountid)
            ) T1
        LEFT OUTER JOIN
            (SELECT lower(mainaccountid) as mainaccountid,
                    max(globalsn) as openid
             FROM bf_extract.beanfundb_bfapp_mainaccount
             WHERE 1 = 1
             AND ('[:DateNoLine]' < '20200701' AND dt = '20200701')
             OR ('[:DateNoLine]' >='20200701' AND dt >= '[:DateNoLine]' AND dt <= DATE_FORMAT(DATE_ADD('[:DateLine]', 2),'yyyyMMdd'))
             GROUP BY lower(mainaccountid)
            ) T2
        ON T1.mainaccountid = T2.mainaccountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]


    @classmethod
    def insert16001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 16001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='16001')
            SELECT 
                t1.user_id as CommonData_1
                , t1.order_no  as CommonData_2
                , t3.product_no as CommonData_3
                , t3.product_name as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , t1.total_price as CommonData_11
                , t2.order_quantity as CommonData_12
                , t2.sale_price as CommonData_13
                , t3.product_price as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , null as UniqueInt_2
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
                , t1.order_time as UniqueTime_1
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
            FROM 
                ( SELECT distinct concat(from_unixtime(unix_timestamp(order_date, 'yyyyMMdd'), 'yyyy-MM-dd'), ' ', order_time) as order_time,
                          user_id,
                          order_no,
                          total_price
                  FROM cso_extract.db_shop_sst_orders
                  WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                  AND dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',5),'yyyyMMdd')
                  AND order_date = '[:DateNoLine]'
            ) t1
            INNER JOIN 
                ( SELECT distinct
                         order_no,
                         product_no,
                         order_quantity,
                         sale_price
                  from cso_extract.db_shop_sst_order_details
                  where dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                  and dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',5),'yyyyMMdd')
                ) t2
            on t1.order_no  = t2.order_no
            inner join (select distinct
                               product_no,
                               product_name,
                               product_price
                        from cso_extract.db_shop_sst_products
                        where dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                        and dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',5),'yyyyMMdd')
                       ) t3
            on t2.product_no = t3.product_no
            inner join (select  distinct
                                order_no
                        from cso_extract.db_shop_sst_order_payments
                        where dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                        and dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',5),'yyyyMMdd')
                        and payment_method = 'Gash Point'
                       ) t4
            on t1.order_no = t4.order_no;
        """
        return "OrderInsert", [orderInsertSQLCode1]


    @classmethod
    def insert16002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                -- Make [:GameName] ModelData 16002
                INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='16002')
                SELECT 
                    TMP.accountid as CommonData_1
                    , TMP.item_id  as CommonData_2
                    , TMP.item_name as CommonData_3
                    , null as CommonData_4
                    , null as CommonData_5
                    , null as CommonData_6
                    , null as CommonData_7
                    , null as CommonData_8
                    , null as CommonData_9
                    , null as CommonData_10
                    , TMP.item_quantity as CommonData_11
                    , TMP.item_price as CommonData_12
                    , TMP.total_payment as CommonData_13
                    , null as CommonData_14
                    , null as CommonData_15
                    , null as UniqueInt_1
                    , null as UniqueInt_2
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
                    , TMP.purchase_time as UniqueTime_1
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
                FROM(
                    SELECT CommonData_1 as accountid,
                           CommonData_3 AS item_id,
                           CommonData_4 AS item_name,
                           CommonData_12 AS item_quantity,
                           case when CommonData_14 is not null then CommonData_14 else CommonData_13/CommonData_12 end AS item_price,
                           CommonData_13 AS total_payment,
                           from_unixtime(unix_timestamp(UniqueTime_1, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') AS purchase_time
                    FROM gtwpd.modelextract_modelextract
                    WHERE 1 = 1
                    AND dt = '[:DateNoLine]'
                    AND game = 'cso'
                    AND tablenumber = 16001
                ) TMP;
            """
        return "OrderInsert", [orderInsertSQLCode1]

