import time

class ModelExtract_2000() :

    @classmethod
    def insert2001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2001')
            SELECT
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.gender as UniqueInt_1
                , BB.c_skin as UniqueInt_2
                , BB.c_face as UniqueInt_3
                , BB.c_hair as UniqueInt_4
                , BB.b_job as UniqueInt_5
                , BB.p_map as UniqueInt_6
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
                , null as UniqueTime_1
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
            INNER JOIN maple_extract.gamedb_character BB ON 1 = 1
                AND AA.CommonData_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003  ; 
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2002')
            SELECT
                AAAA.serviceid as CommonData_1
                , AAAA.accountid as CommonData_2
                , AAAA.characterid as CommonData_3
                , AAAA.charactername as CommonData_4
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
                , BBBB.allexp + AAAA.exp_i as UniqueInt_1
                , CCCC.allexp + AAAA.exp_f as UniqueInt_2
                , CCCC.allexp + AAAA.exp_f - BBBB.allexp - AAAA.exp_i as UniqueInt_3
                , AAAA.level_i as UniqueInt_4
                , AAAA.level_f as UniqueInt_5
                , AAAA.level_f - AAAA.level_i  as UniqueInt_6
                , AAAA.pop_i as UniqueInt_7
                , AAAA.pop_f as UniqueInt_8
                , AAAA.pop_f - AAAA.pop_i  as UniqueInt_9
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
                , null as UniqueTime_1
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
            FROM (
                SELECT 
                    AAA.serviceid as serviceid
                    , AAA.accountid  as accountid
                    , AAA.characterid as characterid
                    , AAA.charactername as charactername
                    , MAX(AAA.level_i) AS level_i
                    , MAX(AAA.exp_i) AS exp_i
                    , MAX(AAA.pop_i) AS pop_i
                    , MAX(AAA.level_f) AS level_f
                    , MAX(AAA.exp_f) AS exp_f
                    , MAX(AAA.pop_f) AS pop_f
                FROM ( 
                    SELECT
                        AA.Commondata_1 as serviceid
                        , AA.CommonData_2  as accountid
                        , AA.Commondata_3 as characterid
                        , AA.CommonData_4 as charactername
                        , BB.b_level AS level_i
                        , BB.s_exp AS exp_i
                        , BB.s_pop AS pop_i
                        , 0 AS level_f
                        , 0 AS exp_f
                        , 0 AS pop_f
                    FROM gtwpd.modelextract_modelextract AA
                    INNER JOIN maple_all.gamedb_character BB ON 1 = 1
                        AND AA.commondata_3 = BB.characterid
                        AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                        AND BB.world = '[:World]'
                    WHERE 1 = 1 
                        AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                        AND AA.world = '[:World]'
                        AND AA.tablenumber = 11003
                    UNION ALL
                    SELECT
                        AA.Commondata_1 as serviceid
                        , AA.CommonData_2  as accountid
                        , AA.Commondata_3 as characterid
                        , AA.CommonData_4 as charactername
                        , 0 AS level_i
                        , 0 AS exp_i
                        , 0 AS pop_i
                        , BB.b_level AS level_f
                        , BB.s_exp AS exp_f
                        , BB.b_level AS pop_f
                    FROM gtwpd.modelextract_modelextract  AA
                    INNER JOIN maple_extract.gamedb_character BB ON 1 = 1
                        AND AA.commondata_3 = BB.characterid
                        AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                        AND BB.world = '[:World]'
                    WHERE 1 = 1 
                        AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                        AND AA.world = '[:World]'
                        AND AA.tablenumber = 11003
                ) AAA
                GROUP BY
                    AAA.serviceid 
                    , AAA.accountid  
                    , AAA.characterid
                    , AAA.charactername 
            )AAAA
            INNER JOIN (
                SELECT 
                    CAST(BB.uniqueint_1 AS BIGINT) AS level
                    , CAST(BB.uniqueint_3 AS BIGINT) AS allexp
                FROM gtwpd.modelextract_modelextract BB
                WHERE 1 = 1 
                    AND BB.tablenumber='12051'
                    AND BB.game = 'maple'
                    AND BB.dt in( 
                        SELECT 
                            max(dt) AS 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE 1 = 1 
                            AND tablenumber='12051'
                            AND game = 'maple'
                            AND world = 'COMMON'
                    )
                    AND BB.world = 'COMMON'
            ) BBBB ON 1 = 1 
                AND AAAA.level_i = BBBB.level 
            INNER JOIN (
                SELECT 
                    CAST(CC.uniqueint_1 AS BIGINT) AS level
                    , CAST(CC.uniqueint_3 AS BIGINT) AS allexp
                FROM gtwpd.modelextract_modelextract CC
                WHERE 1 = 1 
                    AND CC.tablenumber='12051'
                    AND CC.game = 'maple'
                    AND CC.dt in( 
                        SELECT 
                            max(dt) AS 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE 1 = 1 
                            AND tablenumber='12051'
                            AND game = 'maple'
                            AND world = 'COMMON'
                    )
                    AND CC.world = 'COMMON'
            ) CCCC ON 1 = 1 
                AND AAAA.level_f = CCCC.level ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2101DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2101
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2101')
            SELECT
                AA.Commondata_1 as CommonData_1
                , AA.Commondata_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.mhp as UniqueInt_1
                , BB.mmp as UniqueInt_2
                , BB.str as UniqueInt_3
                , BB.dex as UniqueInt_4
                , BB.int as UniqueInt_5
                , BB.luk as UniqueInt_6
                , BB.statdamagemin as UniqueInt_7
                , BB.statdamagemax as UniqueInt_8
                , BB.criticalprop as UniqueInt_9
                , BB.criticalminr as UniqueInt_10
                , BB.criticalmaxr as UniqueInt_11
                , BB.bossdamager as UniqueInt_12
                , BB.ignoredef as UniqueInt_13
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
                , null as UniqueTime_1
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
            INNER JOIN maple_all.gamedb_characterstatinfo BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2102DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2102
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2102')
            SELECT
                AA.Commondata_1 as CommonData_1
                , AA.Commondata_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.asrr as UniqueInt_1
                , BB.stanceprop as UniqueInt_2
                , BB.pdd as UniqueInt_3
                , BB.mdd as UniqueInt_4
                , BB.pacc as UniqueInt_5
                , BB.macc as UniqueInt_6
                , BB.peva as UniqueInt_7
                , BB.meva as UniqueInt_8
                , BB.speed as UniqueInt_9
                , BB.jump as UniqueInt_10
                , BB.starforce as UniqueInt_11
                , BB.totaldamr as UniqueInt_12
                , BB.finaldamr as UniqueInt_13
                , BB.arcaneforce as UniqueInt_14
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
                , null as UniqueTime_1
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
            INNER JOIN maple_all.gamedb_characterstatinfo BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2103DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2103
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2103')
            SELECT
                AA.Commondata_1 as CommonData_1
                , AA.Commondata_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.s_charismaexp as UniqueInt_1
                , BB.s_insightexp as UniqueInt_2
                , BB.s_willexp as UniqueInt_3
                , BB.s_craftexp as UniqueInt_4
                , BB.s_senseexp as UniqueInt_5
                , BB.s_charmexp as UniqueInt_6
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
                , null as UniqueTime_1
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
            INNER JOIN maple_all.gamedb_character BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003  ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2111DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2111
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2111')
            SELECT
                AA.Commondata_1 as CommonData_1
                , AA.Commondata_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.mhp as UniqueInt_1
                , BB.mmp as UniqueInt_2
                , BB.str as UniqueInt_3
                , BB.dex as UniqueInt_4
                , BB.int as UniqueInt_5
                , BB.luk as UniqueInt_6
                , BB.statdamagemin as UniqueInt_7
                , BB.statdamagemax as UniqueInt_8
                , BB.criticalprop as UniqueInt_9
                , BB.criticalminr as UniqueInt_10
                , BB.criticalmaxr as UniqueInt_11
                , BB.bossdamager as UniqueInt_12
                , BB.ignoredef as UniqueInt_13
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
                , null as UniqueTime_1
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
            INNER JOIN maple_extract.gamedb_characterstatinfo BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003 ;

        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2112DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2112
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2112')
            SELECT
                AA.Commondata_1 as CommonData_1
                , AA.Commondata_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.asrr as UniqueInt_1
                , BB.stanceprop as UniqueInt_2
                , BB.pdd as UniqueInt_3
                , BB.mdd as UniqueInt_4
                , BB.pacc as UniqueInt_5
                , BB.macc as UniqueInt_6
                , BB.peva as UniqueInt_7
                , BB.meva as UniqueInt_8
                , BB.speed as UniqueInt_9
                , BB.jump as UniqueInt_10
                , BB.starforce as UniqueInt_11
                , BB.totaldamr as UniqueInt_12
                , BB.finaldamr as UniqueInt_13
                , BB.arcaneforce as UniqueInt_14
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
                , null as UniqueTime_1
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
            INNER JOIN maple_extract.gamedb_characterstatinfo BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert2113DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2113
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2113')
            SELECT
                AA.Commondata_1 as CommonData_1
                , AA.Commondata_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , BB.s_charismaexp as UniqueInt_1
                , BB.s_insightexp as UniqueInt_2
                , BB.s_willexp as UniqueInt_3
                , BB.s_craftexp as UniqueInt_4
                , BB.s_senseexp as UniqueInt_5
                , BB.s_charmexp as UniqueInt_6
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
                , null as UniqueTime_1
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
            INNER JOIN maple_extract.gamedb_character BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND BB.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003  ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

