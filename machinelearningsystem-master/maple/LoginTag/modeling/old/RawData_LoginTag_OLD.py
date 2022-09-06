import os
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv


class RawData_LoginTag_OLD() :

    @classmethod
    def MakeRawData_LoginTag_R0_0_1(self, makeInfo):
        orderSQL1 = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:RawDataVersion]' , dt = '[:DateNoLine]' , step = 'RawData')
            SELECT 
                AA.commondata_1
                , AA.commondata_5 
                , AA.commondata_6
                , ROUND(SUM(CASE WHEN AAA.weekday IN (0,6) THEN AAA.m ELSE 0 END ) / 2,5) AS m_holiday
                , ROUND(SUM(CASE WHEN AAA.weekday IN (0,6) THEN AAA.a ELSE 0 END ) / 2,5) AS a_holiday
                , ROUND(SUM(CASE WHEN AAA.weekday IN (0,6) THEN AAA.e ELSE 0 END ) / 2,5) AS e_holiday
                , ROUND(SUM(CASE WHEN AAA.weekday NOT IN (0,6) THEN AAA.m ELSE 0 END ) / 5,5) AS m_weekday
                , ROUND(SUM(CASE WHEN AAA.weekday NOT IN (0,6) THEN AAA.a ELSE 0 END ) / 5,5) AS a_weekday
                , ROUND(SUM(CASE WHEN AAA.weekday NOT IN (0,6) THEN AAA.e ELSE 0 END ) / 5,5) AS e_weekday
            FROM (
                SELECT 
                    AA.commondata_1 AS commondata_1 
                    , AA.commondata_5 AS commondata_5 
                    , AA.commondata_6 AS commondata_6
                    , (datediff(concat_ws('-',substr(AA.dt,1,4),substr(AA.dt,5,2),substr(AA.dt,7,2)) ,'2000-01-01')-1) %7 AS weekday 
                    , SUM(CASE 
                        WHEN AA.tablenumber = '1132' THEN AA.UniqueInt_1 + AA.UniqueInt_2 + AA.UniqueInt_3 + AA.UniqueInt_4 + AA.UniqueInt_5 + AA.UniqueInt_6 + AA.UniqueInt_7 + AA.UniqueInt_8
                        WHEN AA.tablenumber = '1133' THEN 0
                        ELSE 0 
                      END) /28800 / ((datediff(
                        concat_ws('-',substr('20220107',1,4),substr('20220107',5,2),substr('20220107',7,2))
                        ,concat_ws('-',substr('20220101',1,4),substr('20220101',5,2),substr('20220101',7,2)) 
                      )+1)/7) AS m
                    , SUM(CASE 
                        WHEN AA.tablenumber = '1132' THEN AA.UniqueInt_9 + AA.UniqueInt_10 + AA.UniqueInt_11 + AA.UniqueInt_12
                        WHEN AA.tablenumber = '1133' THEN AA.UniqueInt_1 + AA.UniqueInt_2 + AA.UniqueInt_3 + AA.UniqueInt_4
                        ELSE 0 
                      END) /28800 / ((datediff(
                        concat_ws('-',substr('20220107',1,4),substr('20220107',5,2),substr('20220107',7,2))
                        ,concat_ws('-',substr('20220101',1,4),substr('20220101',5,2),substr('20220101',7,2)) 
                      )+1)/7) AS a
                    , SUM(CASE 
                        WHEN AA.tablenumber = '1132' THEN 0 
                        WHEN AA.tablenumber = '1133' THEN AA.UniqueInt_5 + AA.UniqueInt_6 + AA.UniqueInt_7 + AA.UniqueInt_8 + AA.UniqueInt_9 + AA.UniqueInt_10 + AA.UniqueInt_11 + AA.UniqueInt_12
                        ELSE 0 
                      END) /28800 / ((datediff(
                        concat_ws('-',substr('20220107',1,4),substr('20220107',5,2),substr('20220107',7,2))
                        ,concat_ws('-',substr('20220101',1,4),substr('20220101',5,2),substr('20220101',7,2)) 
                      )+1)/7) AS e
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1 
                    AND AA.game = 'maple'
                    AND AA.tablenumber IN ('1132','1133')
                    AND AA.dt >= '20220101'
                    AND AA.dt <= '20220107'
                GROUP BY 
                    AA.commondata_1 
                    , AA.commondata_5 
                    , AA.commondata_6 
                    , (datediff( concat_ws('-',substr(AA.dt,1,4),substr(AA.dt,5,2),substr(AA.dt,7,2)) ,'2000-01-01')-1) %7
            ) AAA
            GROUP BY 
                AAA.commondata_1
                , AAA.commondata_5 
                , AAA.commondata_6
        """
        return "MakePreProcessOrderSQLInsert", [orderSQL1]






