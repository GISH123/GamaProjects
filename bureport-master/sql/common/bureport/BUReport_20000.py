import time

cycleDict = {'M': '[:DateNoLineFirstMonth]',
             'W': '[:DateNoLineFirstWeek]'}
class BUReport_20000 :

    # PCA
    @classmethod
    def insert21001DataSQL(self, makeInfo):
        makeInfo = { 'tableNumber': 21001 , 'projectName': 'tag', 'version': '2_0_0','modelName' : 'login' , 'world' : False , 'percent' : 0.1,  'joinType': 'LEFT', 'key': 'Commondata_2'}
        return  self.__insertPCASQL(makeInfo=  makeInfo)
    @classmethod
    def insert22001DataSQL(self, makeInfo):
        return self.__insertPCASQL(makeInfo={'tableNumber': 22001, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'growth','world' : False,  'percent' : 0.1,
                                           'joinType': 'LEFT', 'key': 'Commondata_2'})
    @classmethod
    def insert22002DataSQL(self, makeInfo):
        return self.__insertPCASQL(makeInfo={'tableNumber': 22002, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'fight', 'world' : True, 'percent' : 0.1,
                                           'joinType': 'LEFT', 'key': 'Commondata_3'})
    @classmethod
    def insert27001DataSQL(self, makeInfo):
        return self.__insertPCASQL(makeInfo={'tableNumber': 27001, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'rank_union', 'world' : True, 'percent' : 0.1,
                                           'joinType': 'INNER', 'key': 'Commondata_2'})
    @classmethod
    def insert27002DataSQL(self, makeInfo):
        return self.__insertPCASQL(makeInfo={'tableNumber': 27002, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'rank_dojang', 'world' : True, 'percent' : 0.1,
                                           'joinType': 'INNER', 'key': 'Commondata_3', 'cycle': 'W'})

    # LDA
    @classmethod
    def insert24001DataSQL(self, makeInfo):
        return self.__insertLDASQL(makeInfo={'tableNumber': 24001, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'quest', 'world' : True, 'percent' :  "1/7",
                                           'joinType': 'INNER', 'key': 'Commondata_3'})

    # SNA
    @classmethod
    def insert25001DataSQL(self, makeInfo):
        return self.__insertSNASQL(makeInfo={'tableNumber': 25001, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'friend', 'world' : True, 'percent' : 0.1,
                                           'joinType': 'INNER', 'key': 'Commondata_3'})
    @classmethod
    def insert26001DataSQL(self, makeInfo):
        return self.__insertSNASQL(makeInfo={'tableNumber': 26001, 'projectName': 'tag', 'version': '2_0_0',
                                           'modelName': 'exchange', 'world' : True, 'percent' : 0.1,
                                           'joinType': 'INNER', 'key': 'Commondata_3'})



    # NoModel 獨立處理
    @classmethod
    def insert26002DataSQL(self, makeInfo):
        makeInfo = {'tableNumber': 26002, 'projectName': 'tag', 'modelName': 'auction', 'version': '2_0_0',  'percent': 0.1, 'world' : True, 'joinType': 'INNER', 'key': 'Commondata_3'}
        worldStr = '[:World]' if makeInfo['world'] else 'COMMON'
        worldStr2 = 'AND world = [:World]' if makeInfo['world'] else ''
        firstDtStr =  cycleDict[makeInfo.setdefault('cycle', 'M')]

        orderInsertSQLCode1 = f"""
            -- Make [:GameName] BU {makeInfo['tableNumber']}
            WITH  tb AS( SELECT
                percentile_approx(uniquefloat_001, {1 - makeInfo['percent'] }) AS p_UniqueFloat_001 
            ,   percentile_approx(uniquefloat_002, {1 - makeInfo['percent'] }) AS p_UniqueFloat_002
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step =  'preprocess'
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}'
            ) 
            INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='{worldStr}',tablenumber='{makeInfo['tableNumber']}')
            SELECT
                  MAX(NVL(AAA.CommonData_1  , BBB.CommonData_1 )) as CommonData_1
                , MAX(NVL(AAA.CommonData_2  , BBB.CommonData_2 ))  as CommonData_2
                , MAX(NVL(AAA.CommonData_3  , BBB.CommonData_3 )) as CommonData_3
                , MAX(NVL(AAA.CommonData_4  , BBB.CommonData_4 )) as CommonData_4
                , MAX(NVL(AAA.CommonData_5  , BBB.CommonData_5 )) as CommonData_5
                , MAX(NVL(AAA.CommonData_6  , BBB.CommonData_6 )) as CommonData_6
                , MAX(NVL(AAA.CommonData_7  , BBB.CommonData_7 )) as CommonData_7
                , MAX(NVL(AAA.CommonData_8  , BBB.CommonData_8 )) as CommonData_8
                , MAX(NVL(AAA.CommonData_9  , BBB.CommonData_9 )) as CommonData_9
                , MAX(NVL(AAA.CommonData_10 , BBB.CommonData_10))  as CommonData_10
                , MAX(NVL(AAA.CommonData_11 , BBB.CommonData_11))  as CommonData_11
                , MAX(NVL(AAA.CommonData_12 , BBB.CommonData_12))  as CommonData_12
                , MAX(NVL(AAA.CommonData_13 , BBB.CommonData_13))  as CommonData_13
                , MAX(NVL(AAA.CommonData_14 , BBB.CommonData_14))  as CommonData_14
                , MAX(NVL(AAA.CommonData_15 , BBB.CommonData_15))  as CommonData_15
                , MAX(uniquefloat_001) as UniqueInt_1 -- 販賣
                , MAX(uniquefloat_002) as UniqueInt_2 -- 購買
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
                , MAX(CASE WHEN UniqueFloat_001 >= p_UniqueFloat_001 THEN 1 ELSE 0 END) as UniqueStr_1 -- 販賣
                , MAX(CASE WHEN UniqueFloat_002 >= p_UniqueFloat_002 THEN 1 ELSE 0 END) as UniqueStr_2 -- 購買
                , NULL as UniqueStr_3
                , NULL as UniqueStr_4
                , NULL as UniqueStr_5
                , NULL as UniqueStr_6
                , NULL as UniqueStr_7
                , NULL as UniqueStr_8
                , NULL as UniqueStr_9
                , NULL as UniqueStr_10
                , NULL as UniqueStr_11
                , NULL as UniqueStr_12
                , NULL as UniqueStr_13
                , NULL as UniqueStr_14
                , NULL as UniqueStr_15
                , NULL as UniqueStr_16
                , NULL as UniqueStr_17
                , NULL as UniqueStr_18
                , NULL as UniqueStr_19
                , NULL as UniqueStr_20
                , NULL as UniqueDBL_1 
                , NULL as UniqueDBL_2 
                , NULL as UniqueDBL_3
                , NULL as UniqueDBL_4
                , NULL as UniqueDBL_5
                , NULL as UniqueDBL_6
                , NULL as UniqueDBL_7
                , NULL as UniqueDBL_8
                , NULL as UniqueDBL_9
                , NULL as UniqueDBL_10
                , NULL as UniqueDBL_11
                , NULL as UniqueDBL_12
                , NULL as UniqueDBL_13
                , NULL as UniqueDBL_14
                , NULL as UniqueDBL_15
                , NULL as UniqueDBL_16
                , NULL as UniqueDBL_17
                , NULL as UniqueDBL_18
                , NULL as UniqueDBL_19
                , NULL as UniqueDBL_20
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
                tb 
            INNER JOIN
             (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract 
                WHERE 1=1
                 AND dt = '[:DateNoLine]'
                 {worldStr2}
                 AND tablenumber = 11003
                 AND game='[:GameName]'
             )AAA
             {makeInfo['joinType']} JOIN 
             (
            SELECT 
                *
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step =  'preprocess'
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}' 
               ) BBB 
               ON AAA.{makeInfo['key']} = BBB.{makeInfo['key']}
            GROUP BY AAA.{makeInfo['key']};
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert25002DataSQL(self, makeInfo):
        makeInfo = {'tableNumber': 25002, 'projectName': 'tag', 'modelName': 'guild', 'version': '2_0_0',  'world' : True, 'joinType': 'INNER', 'key': 'Commondata_3'}
        worldStr = '[:World]' if makeInfo['world'] else 'COMMON'
        worldStr2 = 'AND world = [:World]' if makeInfo['world'] else ''
        firstDtStr =  cycleDict[makeInfo.setdefault('cycle', 'M')]
        orderInsertSQLCode1 = f"""
            -- Make [:GameName] BU {makeInfo['tableNumber']}
            WITH  tb AS( SELECT
            	commondata_8  AS guildid
                ,  AVG(UniqueFloat_001) AS guild_comm
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step =  'preprocess'
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}'
            	AND commondata_8 IS NOT NULL
            GROUP BY commondata_8  
            ) 
            INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='{worldStr}',tablenumber='{makeInfo['tableNumber']}')
            SELECT
                  MAX(NVL(AAA.CommonData_1  , BBB.CommonData_1 )) as CommonData_1
                , MAX(NVL(AAA.CommonData_2  , BBB.CommonData_2 ))  as CommonData_2
                , MAX(NVL(AAA.CommonData_3  , BBB.CommonData_3 )) as CommonData_3
                , MAX(NVL(AAA.CommonData_4  , BBB.CommonData_4 )) as CommonData_4
                , MAX(NVL(AAA.CommonData_5  , BBB.CommonData_5 )) as CommonData_5
                , MAX(NVL(AAA.CommonData_6  , BBB.CommonData_6 )) as CommonData_6
                , MAX(NVL(AAA.CommonData_7  , BBB.CommonData_7 )) as CommonData_7
                , MAX(NVL(AAA.CommonData_8  , BBB.CommonData_8 )) as CommonData_8
                , MAX(NVL(AAA.CommonData_9  , BBB.CommonData_9 )) as CommonData_9
                , MAX(NVL(AAA.CommonData_10 , BBB.CommonData_10))  as CommonData_10
                , MAX(NVL(AAA.CommonData_11 , BBB.CommonData_11))  as CommonData_11
                , MAX(NVL(AAA.CommonData_12 , BBB.CommonData_12))  as CommonData_12
                , MAX(NVL(AAA.CommonData_13 , BBB.CommonData_13))  as CommonData_13
                , MAX(NVL(AAA.CommonData_14 , BBB.CommonData_14))  as CommonData_14
                , MAX(NVL(AAA.CommonData_15 , BBB.CommonData_15))  as CommonData_15
                , MAX(uniquefloat_001) as UniqueInt_1 -- 公會貢獻
                , MIN(uniquefloat_002) as UniqueInt_2 -- 公會lvl
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
                , MAX(CASE WHEN UniqueFloat_001 >= guild_comm THEN 1 ELSE 0 END) as UniqueStr_1 -- 超越公會平均
                , MAX(CASE WHEN UniqueFloat_002 <= 2 THEN 1 ELSE 0 END) as UniqueStr_2 -- 公會長
                , NULL as UniqueStr_3
                , NULL as UniqueStr_4
                , NULL as UniqueStr_5
                , NULL as UniqueStr_6
                , NULL as UniqueStr_7
                , NULL as UniqueStr_8
                , NULL as UniqueStr_9
                , NULL as UniqueStr_10
                , NULL as UniqueStr_11
                , NULL as UniqueStr_12
                , NULL as UniqueStr_13
                , NULL as UniqueStr_14
                , NULL as UniqueStr_15
                , NULL as UniqueStr_16
                , NULL as UniqueStr_17
                , NULL as UniqueStr_18
                , NULL as UniqueStr_19
                , NULL as UniqueStr_20
                , NULL as UniqueDBL_1 
                , NULL as UniqueDBL_2 
                , NULL as UniqueDBL_3
                , NULL as UniqueDBL_4
                , NULL as UniqueDBL_5
                , NULL as UniqueDBL_6
                , NULL as UniqueDBL_7
                , NULL as UniqueDBL_8
                , NULL as UniqueDBL_9
                , NULL as UniqueDBL_10
                , NULL as UniqueDBL_11
                , NULL as UniqueDBL_12
                , NULL as UniqueDBL_13
                , NULL as UniqueDBL_14
                , NULL as UniqueDBL_15
                , NULL as UniqueDBL_16
                , NULL as UniqueDBL_17
                , NULL as UniqueDBL_18
                , NULL as UniqueDBL_19
                , NULL as UniqueDBL_20
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
             (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract 
                WHERE 1=1
                 AND dt = '[:DateNoLine]'
                 {worldStr2}
                 AND tablenumber = 11003
                 AND game='[:GameName]'
             )AAA
             {makeInfo['joinType']} JOIN 
             (
            SELECT 
                *
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step =  'preprocess'
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}' 
               ) BBB 
               ON AAA.{makeInfo['key']} = BBB.{makeInfo['key']}
            INNER JOIN tb 
                ON BBB.CommonData_8 = tb.guildid
            GROUP BY AAA.{makeInfo['key']};
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insertPCASQL(self, makeInfo):
        worldStr = '[:World]' if makeInfo['world'] else 'COMMON'
        worldStr2 = 'AND world = [:World]' if makeInfo['world'] else ''
        firstDtStr = cycleDict[makeInfo.setdefault('cycle', 'M')]
        orderInsertSQLCode1 = f"""
            -- Make [:GameName] BU {makeInfo['tableNumber']}
            INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='{worldStr}',tablenumber='{makeInfo['tableNumber']}')
            SELECT
                  MAX(NVL(AAA.CommonData_1  , BBB.CommonData_1 )) as CommonData_1
                , MAX(NVL(AAA.CommonData_2  , BBB.CommonData_2 ))  as CommonData_2
                , MAX(NVL(AAA.CommonData_3  , BBB.CommonData_3 )) as CommonData_3
                , MAX(NVL(AAA.CommonData_4  , BBB.CommonData_4 )) as CommonData_4
                , MAX(NVL(AAA.CommonData_5  , BBB.CommonData_5 )) as CommonData_5
                , MAX(NVL(AAA.CommonData_6  , BBB.CommonData_6 )) as CommonData_6
                , MAX(NVL(AAA.CommonData_7  , BBB.CommonData_7 )) as CommonData_7
                , MAX(NVL(AAA.CommonData_8  , BBB.CommonData_8 )) as CommonData_8
                , MAX(NVL(AAA.CommonData_9  , BBB.CommonData_9 )) as CommonData_9
                , MAX(NVL(AAA.CommonData_10 , BBB.CommonData_10))  as CommonData_10
                , MAX(NVL(AAA.CommonData_11 , BBB.CommonData_11))  as CommonData_11
                , MAX(NVL(AAA.CommonData_12 , BBB.CommonData_12))  as CommonData_12
                , MAX(NVL(AAA.CommonData_13 , BBB.CommonData_13))  as CommonData_13
                , MAX(NVL(AAA.CommonData_14 , BBB.CommonData_14))  as CommonData_14
                , MAX(NVL(AAA.CommonData_15 , BBB.CommonData_15))  as CommonData_15
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
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_001 >= 1 - {makeInfo['percent']}  THEN 1 ELSE 0 END) as UniqueStr_1
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_002 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_2
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_003 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_3
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_004 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_4
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_005 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_5
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_006 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_6
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_007 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_7
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_008 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_8
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_009 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_9
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_010 >= 1 - {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_10
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_001 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_11
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_002 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_12
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_003 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_13
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_004 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_14
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_005 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_15
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_006 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_16
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_007 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_17
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_008 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_18
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_009 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_19
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_010 <= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_20
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_001 END) as UniqueDBL_1
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_002 END) as UniqueDBL_2
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_003 END) as UniqueDBL_3
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_004 END) as UniqueDBL_4
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_005 END) as UniqueDBL_5
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_006 END) as UniqueDBL_6
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_007 END) as UniqueDBL_7
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_008 END) as UniqueDBL_8
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_009 END) as UniqueDBL_9
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_010 END) as UniqueDBL_10
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_001 END) as UniqueDBL_11
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_002 END) as UniqueDBL_12
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_003 END) as UniqueDBL_13
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_004 END) as UniqueDBL_14
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_005 END) as UniqueDBL_15
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_006 END) as UniqueDBL_16
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_007 END) as UniqueDBL_17
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_008 END) as UniqueDBL_18
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_009 END) as UniqueDBL_19
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_010 END) as UniqueDBL_20
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
             (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract 
                WHERE 1=1
                 AND dt = '[:DateNoLine]'
                 {worldStr2}
                 AND tablenumber = 11003
                 AND game='[:GameName]'
             )AAA
             {makeInfo['joinType']} JOIN 
             (
            SELECT 
                *
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step IN ( 'result', 'percentile')
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}' 
               ) BBB 
               ON AAA.{makeInfo['key']} = BBB.{makeInfo['key']}
            GROUP BY AAA.{makeInfo['key']}
            ; 
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insertLDASQL(self, makeInfo):
        worldStr = '[:World]' if makeInfo['world'] else 'COMMON'
        worldStr2 = 'AND world = [:World]' if makeInfo['world'] else ''
        firstDtStr = cycleDict[makeInfo.setdefault('cycle', 'M')]
        orderInsertSQLCode1 = f"""
            -- Make [:GameName] BU {makeInfo['tableNumber']}
            INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='{worldStr}',tablenumber='{makeInfo['tableNumber']}')
            SELECT
                  MAX(NVL(AAA.CommonData_1  , BBB.CommonData_1 )) as CommonData_1
                , MAX(NVL(AAA.CommonData_2  , BBB.CommonData_2 ))  as CommonData_2
                , MAX(NVL(AAA.CommonData_3  , BBB.CommonData_3 )) as CommonData_3
                , MAX(NVL(AAA.CommonData_4  , BBB.CommonData_4 )) as CommonData_4
                , MAX(NVL(AAA.CommonData_5  , BBB.CommonData_5 )) as CommonData_5
                , MAX(NVL(AAA.CommonData_6  , BBB.CommonData_6 )) as CommonData_6
                , MAX(NVL(AAA.CommonData_7  , BBB.CommonData_7 )) as CommonData_7
                , MAX(NVL(AAA.CommonData_8  , BBB.CommonData_8 )) as CommonData_8
                , MAX(NVL(AAA.CommonData_9  , BBB.CommonData_9 )) as CommonData_9
                , MAX(NVL(AAA.CommonData_10 , BBB.CommonData_10))  as CommonData_10
                , MAX(NVL(AAA.CommonData_11 , BBB.CommonData_11))  as CommonData_11
                , MAX(NVL(AAA.CommonData_12 , BBB.CommonData_12))  as CommonData_12
                , MAX(NVL(AAA.CommonData_13 , BBB.CommonData_13))  as CommonData_13
                , MAX(NVL(AAA.CommonData_14 , BBB.CommonData_14))  as CommonData_14
                , MAX(NVL(AAA.CommonData_15 , BBB.CommonData_15))  as CommonData_15
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
                , MAX(CASE WHEN step = 'result' AND uniquefloat_001 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_1
                , MAX(CASE WHEN step = 'result' AND uniquefloat_002 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_2
                , MAX(CASE WHEN step = 'result' AND uniquefloat_003 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_3
                , MAX(CASE WHEN step = 'result' AND uniquefloat_004 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_4
                , MAX(CASE WHEN step = 'result' AND uniquefloat_005 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_5
                , MAX(CASE WHEN step = 'result' AND uniquefloat_006 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_6
                , MAX(CASE WHEN step = 'result' AND uniquefloat_007 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_7
                , MAX(CASE WHEN step = 'result' AND uniquefloat_008 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_8
                , MAX(CASE WHEN step = 'result' AND uniquefloat_009 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_9
                , MAX(CASE WHEN step = 'result' AND uniquefloat_010 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_10
                , MAX(CASE WHEN step = 'result' AND uniquefloat_011 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_11
                , MAX(CASE WHEN step = 'result' AND uniquefloat_012 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_12
                , MAX(CASE WHEN step = 'result' AND uniquefloat_013 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_13
                , MAX(CASE WHEN step = 'result' AND uniquefloat_014 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_14
                , MAX(CASE WHEN step = 'result' AND uniquefloat_015 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_15
                , MAX(CASE WHEN step = 'result' AND uniquefloat_016 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_16
                , MAX(CASE WHEN step = 'result' AND uniquefloat_017 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_17
                , MAX(CASE WHEN step = 'result' AND uniquefloat_018 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_18
                , MAX(CASE WHEN step = 'result' AND uniquefloat_019 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_19
                , MAX(CASE WHEN step = 'result' AND uniquefloat_020 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_20
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_001 END) as UniqueDBL_1
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_002 END) as UniqueDBL_2
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_003 END) as UniqueDBL_3
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_004 END) as UniqueDBL_4
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_005 END) as UniqueDBL_5
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_006 END) as UniqueDBL_6
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_007 END) as UniqueDBL_7
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_008 END) as UniqueDBL_8
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_009 END) as UniqueDBL_9
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_010 END) as UniqueDBL_10
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_011 END) as UniqueDBL_11
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_012 END) as UniqueDBL_12
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_013 END) as UniqueDBL_13
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_014 END) as UniqueDBL_14
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_015 END) as UniqueDBL_15
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_016 END) as UniqueDBL_16
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_017 END) as UniqueDBL_17
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_018 END) as UniqueDBL_18
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_019 END) as UniqueDBL_19
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_020 END) as UniqueDBL_20
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
              (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract 
                WHERE 1=1
                 AND dt = '[:DateNoLine]'
                 {worldStr2}
                 AND tablenumber = 11003
                 AND game='[:GameName]'
             )AAA
             {makeInfo['joinType']} JOIN 
             (
            SELECT 
                *
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step IN ( 'result', 'percentile')
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}' 
               ) BBB 
               ON AAA.{makeInfo['key']} = BBB.{makeInfo['key']}
            GROUP BY AAA.{makeInfo['key']};
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insertSNASQL(self, makeInfo):
        worldStr = '[:World]' if makeInfo['world'] else 'COMMON'
        worldStr2 = 'AND world = [:World]' if makeInfo['world'] else ''
        firstDtStr = cycleDict[makeInfo.setdefault('cycle', 'M')]
        orderInsertSQLCode1 = f"""
            -- Make [:GameName] BU {makeInfo['tableNumber']}
            INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='{worldStr}',tablenumber='{makeInfo['tableNumber']}')
            SELECT
                  MAX(NVL(AAA.CommonData_1  , BBB.CommonData_1 )) as CommonData_1
                , MAX(NVL(AAA.CommonData_2  , BBB.CommonData_2 ))  as CommonData_2
                , MAX(NVL(AAA.CommonData_3  , BBB.CommonData_3 )) as CommonData_3
                , MAX(NVL(AAA.CommonData_4  , BBB.CommonData_4 )) as CommonData_4
                , MAX(NVL(AAA.CommonData_5  , BBB.CommonData_5 )) as CommonData_5
                , MAX(NVL(AAA.CommonData_6  , BBB.CommonData_6 )) as CommonData_6
                , MAX(NVL(AAA.CommonData_7  , BBB.CommonData_7 )) as CommonData_7
                , MAX(NVL(AAA.CommonData_8  , BBB.CommonData_8 )) as CommonData_8
                , MAX(NVL(AAA.CommonData_9  , BBB.CommonData_9 )) as CommonData_9
                , MAX(NVL(AAA.CommonData_10 , BBB.CommonData_10))  as CommonData_10
                , MAX(NVL(AAA.CommonData_11 , BBB.CommonData_11))  as CommonData_11
                , MAX(NVL(AAA.CommonData_12 , BBB.CommonData_12))  as CommonData_12
                , MAX(NVL(AAA.CommonData_13 , BBB.CommonData_13))  as CommonData_13
                , MAX(NVL(AAA.CommonData_14 , BBB.CommonData_14))  as CommonData_14
                , MAX(NVL(AAA.CommonData_15 , BBB.CommonData_15))  as CommonData_15
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
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_001 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_1
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_002 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_2
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_003 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_3
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_004 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_4
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_005 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_5
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_006 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_6
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_007 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_7
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_008 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_8
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_009 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_9
                , MAX(CASE WHEN step = 'percentile' AND uniquefloat_010 >= {makeInfo['percent']} THEN 1 ELSE 0 END) as UniqueStr_10
                , NULL as UniqueStr_11
                , NULL as UniqueStr_12
                , NULL as UniqueStr_13
                , NULL as UniqueStr_14
                , NULL as UniqueStr_15
                , NULL as UniqueStr_16
                , NULL as UniqueStr_17
                , NULL as UniqueStr_18
                , NULL as UniqueStr_19
                , NULL as UniqueStr_20
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_001 END) as UniqueDBL_1
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_002 END) as UniqueDBL_2
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_003 END) as UniqueDBL_3
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_004 END) as UniqueDBL_4
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_005 END) as UniqueDBL_5
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_006 END) as UniqueDBL_6
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_007 END) as UniqueDBL_7
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_008 END) as UniqueDBL_8
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_009 END) as UniqueDBL_9
                , MAX(CASE WHEN step = 'percentile' THEN uniquefloat_010 END) as UniqueDBL_10
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_001 END) as UniqueDBL_11
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_002 END) as UniqueDBL_12
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_003 END) as UniqueDBL_13
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_004 END) as UniqueDBL_14
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_005 END) as UniqueDBL_15
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_006 END) as UniqueDBL_16
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_007 END) as UniqueDBL_17
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_008 END) as UniqueDBL_18
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_009 END) as UniqueDBL_19
                , MAX(CASE WHEN step = 'result' THEN uniquefloat_010 END) as UniqueDBL_20
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
             (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract 
                WHERE 1=1
                 AND dt = '[:DateNoLine]'
                 {worldStr2}
                 AND tablenumber = 11003
                 AND game='[:GameName]'
             )AAA
             {makeInfo['joinType']} JOIN 
             (
            SELECT 
                *
            FROM
                gtwpd.model_data 
            WHERE 1=1 
                AND game='[:GameName]'
                AND project = '{makeInfo['projectName']}'
                AND model = '{makeInfo['modelName']}'
                AND step IN ( 'result', 'percentile')
                AND version = '{makeInfo['version']}'
                AND dt = '{firstDtStr}'
                AND world = '{worldStr}' 
               ) BBB 
               ON AAA.{makeInfo['key']} = BBB.{makeInfo['key']}
            GROUP BY AAA.{makeInfo['key']};
        """
        return "OrderInsert", [orderInsertSQLCode1]

