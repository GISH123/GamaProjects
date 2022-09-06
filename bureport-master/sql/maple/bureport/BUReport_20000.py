from sql.common.bureport.BUReport_20000 import BUReport_20000 as BUReport_20000_Common

class BUReport_20000(BUReport_20000_Common) :

   @classmethod
   def insert21011DataSQL(self, makeInfo):
      orderInsertSQLCode1 = """
         -- Make [:GameName] BU 21011
         INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='21011')
         SELECT
             AAA.CommonData_1 as CommonData_1
             , AAA.CommonData_2  as CommonData_2
             , null as CommonData_3
             , null as CommonData_4
             , AAA.CommonData_5 as CommonData_5
             , AAA.CommonData_6 as CommonData_6
             , CASE WHEN AAA.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
             , null as CommonData_8
             , null as CommonData_9
             , null as CommonData_10
             , null as CommonData_11
             , null as CommonData_12
             , null as CommonData_13
             , null as CommonData_14
             , null as CommonData_15
             -------------
             , BBB.id as UniqueInt_1
             , BBB.cluster as UniqueInt_2
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
             , BBB.type as UniqueStr_1
             , BBB.desc as UniqueStr_2
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
             , null as UniqueTime_1
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
         FROM gtwpd.modelextract_modelextract AAA
         LEFT JOIN (
             SELECT
                 AAA.accountid
                 , AAA.id
                 , BBB.cluster
                 , BBB.TYPE
                 , BBB.desc
             FROM (
                 SELECT
                     AA.accountid
                     , min(BB.id) AS id
                 FROM gtwpd.maple_resultrua_group AA
                 INNER JOIN gtwpd.maple_othertable_acctype BB ON 1 = 1
                     AND AA.cluster = BB.cluster
                 WHERE 1 = 1
                     AND AA.dt = '[:DateNoLineFirstMonth]'
                 GROUP BY
                     AA.accountid
             ) AAA
             LEFT JOIN gtwpd.maple_othertable_acctype BBB ON 1 = 1
                 AND AAA.id = BBB.id
         ) BBB ON 1 = 1
             AND AAA.CommonData_2 = BBB.accountid
         WHERE 1 = 1
             AND AAA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
             AND AAA.tablenumber = 11002 
             AND AAA.game = 'maple';
      """
      return "OrderInsert", [orderInsertSQLCode1]

   @classmethod
   def insert21012DataSQL(self, makeInfo):
      orderInsertSQLCode1 = """
         -- Make [:GameName] BU 0911
         INSERT OVERWRITE TABLE gtwpd.business_bureport PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='21012')
         SELECT
             AAA.CommonData_1 as CommonData_1
             , AAA.CommonData_2  as CommonData_2
             , AAA.CommonData_3 as CommonData_3
             , AAA.CommonData_4 as CommonData_4
             , AAA.CommonData_5 as CommonData_5
             , AAA.CommonData_6 as CommonData_6
             , CASE WHEN AAA.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
             , null as CommonData_8
             , null as CommonData_9
             , null as CommonData_10
             , null as CommonData_11
             , null as CommonData_12
             , null as CommonData_13
             , null as CommonData_14
             , null as CommonData_15
             -------------
             , BBB.id as UniqueInt_1
             , BBB.cluster as UniqueInt_2
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
             , BBB.type as UniqueStr_1
             , BBB.desc as UniqueStr_2
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
             , null as UniqueTime_1
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
             , array(null) as
             , null as UniqueJson_1
         FROM gtwpd.modelextract_modelextract AAA
         LEFT JOIN (
             SELECT
                 AAA.characterid
                 , BBB.id
                 , BBB.cluster
                 , BBB.TYPE
                 , BBB.desc
             FROM (
                 SELECT
                     AA.characterid
                     , min(BB.id) AS id
                 FROM gtwpd.maple_resultrua_group AA
                 INNER JOIN gtwpd.maple_othertable_acctype BB ON 1 = 1
                     AND AA.cluster = BB.cluster
                 WHERE 1 = 1
                     AND AA.dt = '[:DateNoLineFirstMonth]'
                     AND AA.world = '[:World]'
                 GROUP BY
                     AA.characterid
             ) AAA
             LEFT JOIN gtwpd.maple_othertable_acctype BBB ON 1 = 1
                 AND AAA.id = BBB.id
         ) BBB ON 1 = 1
             AND AAA.CommonData_3 = BBB.characterid
         WHERE 1 = 1
             AND AAA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
             AND AAA.world = '[:World]'
             AND AAA.tablenumber = 11003
             AND AAA.game = 'maple';
      """
      return "OrderInsert", [orderInsertSQLCode1]

