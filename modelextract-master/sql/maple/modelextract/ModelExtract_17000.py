import time


class ModelExtract_17000 :

    @classmethod
    def insert17001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 17001 聯盟戰地
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='17001')
            SELECT
                BB.email as CommonData_1
                , AA.accountid  as CommonData_2
                , null as CommonData_3
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
                , AA.unionlevel as UniqueInt_1
                , AA.uniondps as UniqueInt_2
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
                ----------------------------------------
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
                ----------------------------------------
                , null as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM maple_all.gamedb_unionwebranking AA
            INNER JOIN maple_all.globalaccount_account BB ON 1 = 1
                AND BB.dt = '[:DateNoLine]'
                AND AA.accountid = BB.accountid
            WHERE AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]' ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert17002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 17002 武陵道館
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='17002')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , AAAA.characterid as CommonData_3
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
                , AAAA.time_data as UniqueInt_1
                , AAAA.floor_data as UniqueInt_2
                , AAAA.clear_data as UniqueInt_3
                , AAAA.job_data as UniqueInt_4
                , AAAA.dojangRankJob_data as UniqueInt_5
                , AAAA.dojangRank0_data as UniqueInt_6
                , AAAA.dojangRank2_data as UniqueInt_7
                , AAAA.rankrwd_data as UniqueInt_8
                , AAAA.type_data as UniqueInt_9
                , AAAA.percent_data as UniqueInt_10
                , AAAA.rwd_data as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , A_100465_data as UniqueStr_1
                , A_100472_data as UniqueStr_2
                , A_100467_data as UniqueStr_3
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
                ----------------------------------------
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
                ----------------------------------------
                , AAAA.lastrank_data as UniqueTime_1
                , AAAA.lastrwd_data as UniqueTime_2
                , null as UniqueTime_3
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM (
                SELECT
                    AAA.characterid
                    , MAX(CASE WHEN AAA.questid = 100465 THEN AAA.record ELSE NULL END) AS A_100465_data
                    , MAX(CASE WHEN AAA.questid = 100465 THEN AAA.datavalue['Time'] ELSE NULL END) AS time_data
                    , MAX(CASE WHEN AAA.questid = 100465 THEN AAA.datavalue['Floor'] ELSE NULL END) AS floor_data
                    , MAX(CASE WHEN AAA.questid = 100465 THEN AAA.datavalue['Clear'] ELSE NULL END) AS clear_data
                    , MAX(CASE WHEN AAA.questid = 100472 THEN AAA.record ELSE NULL END) AS A_100472_data
                    , MAX(CASE WHEN AAA.questid = 100472 THEN AAA.datavalue['job'] ELSE NULL END) AS job_data
                    , MAX(CASE WHEN AAA.questid = 100472 THEN AAA.datavalue['dojangRankJob'] ELSE NULL END) AS dojangRankJob_data
                    , MAX(CASE WHEN AAA.questid = 100472 THEN AAA.datavalue['dojangRank0'] ELSE NULL END) AS dojangRank0_data
                    , MAX(CASE WHEN AAA.questid = 100472 THEN AAA.datavalue['dojangRank2'] ELSE NULL END) AS dojangRank2_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN AAA.record ELSE NULL END) AS A_100467_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN AAA.datavalue['rankrwd'] ELSE NULL END) AS rankrwd_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN AAA.datavalue['type'] ELSE NULL END) AS type_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN AAA.datavalue['percent'] ELSE NULL END) AS percent_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN AAA.datavalue['rwd'] ELSE NULL END) AS rwd_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN from_unixtime(unix_timestamp(AAA.datavalue['lastrank'],'yy/mm/dd/HH/mm'),'yyyy-MM-dd HH:mm:ss') ELSE NULL END) AS lastrank_data
                    , MAX(CASE WHEN AAA.questid = 100467 THEN from_unixtime(unix_timestamp(AAA.datavalue['lastrwd'],'yy/mm/dd/HH/mm'),'yyyy-MM-dd HH:mm:ss') ELSE NULL END) AS lastrwd_data
                FROM (
                    SELECT
                        AA.characterid
                        , AA.questid
                        , AA.record
                        , str_to_map(AA.record,'[:SEMICOLON]','=') AS datavalue
                    FROM maple_extract.gamedb_questrecordex AA
                    WHERE 1 = 1
                        AND AA.dt = '[:DateNoLine]'
                        AND AA.world = '[:World]'
                        AND AA.questid IN (100465,100467,100472)
                ) AAA
                WHERE 1 = 1
                GROUP BY
                    AAA.characterid
            ) AAAA ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

