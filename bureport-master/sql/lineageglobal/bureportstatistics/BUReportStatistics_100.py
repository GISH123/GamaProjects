from sql.common.bureportstatistics.BUReportStatistics_100 import BUReportStatistics_100 as BUReportStatistics_100_Common

class BUReportStatistics_100(BUReportStatistics_100_Common) :
    pass

    @classmethod
    def make108DataSQL(self, makeInfo):
        insertSQL = """
            WITH basic_data AS (
                SELECT
                    aa.commondata_1 as serviceaccountid
                    , lower(AA.commondata_7) as country
                    , AA.uniquestr_1 AS ip
                FROM [:GameName].bu1806 aa
                WHERE 1 = 1
                    and AA.dt >= '[:StartDateLine]'
                    and AA.dt <= '[:EndDateLine]'
                    AND AA.world = 'COMMON'
                GROUP BY
                    aa.commondata_1
                    , AA.commondata_7
                    , AA.uniquestr_1
            ) , accountiploginaccountdetail AS (
                SELECT
                    AAA.serviceaccountid
                    , max(AAA.country) as country
                    , count(DISTINCT BBB.serviceaccountid) AS iploginaccountcount
                FROM basic_data AAA
                INNER JOIN basic_data BBB ON 1 = 1
                    AND AAA.ip = BBB.ip
                GROUP BY
                    AAA.serviceaccountid
            ) , accountiploginaccount AS (
                SELECT
                    '[:DataReportName]' as dt
                    , AAA.country
                    , sum(1) AS accountcount
                    , sum(CASE WHEN AAA.iploginaccountcount >= 1 AND AAA.iploginaccountcount <= 10 THEN 1 ELSE 0 END) AS accountiploginaccount01_10count
                    , sum(CASE WHEN AAA.iploginaccountcount >= 11 AND AAA.iploginaccountcount <= 30 THEN 1 ELSE 0 END) AS accountiploginaccount11_30count
                    , sum(CASE WHEN AAA.iploginaccountcount >= 31 AND AAA.iploginaccountcount <= 60 THEN 1 ELSE 0 END) AS accountiploginaccount31_60count
                    , sum(CASE WHEN AAA.iploginaccountcount >= 61 AND AAA.iploginaccountcount <= 100 THEN 1 ELSE 0 END) AS accountiploginaccount61_100upcount
                    , sum(CASE WHEN AAA.iploginaccountcount >= 101 THEN 1 ELSE 0 END) AS accountiploginaccount101upcount
                FROM accountiploginaccountdetail AAA
                group by
                    AAA.country
                union all
                SELECT
                    '[:DataReportName]' as dt
                    , 'all' as country
                    , sum(1) AS accountcount
                    , sum(CASE WHEN AAA.iploginaccountcount >= 1 AND AAA.iploginaccountcount <= 10 THEN 1 ELSE 0 END) AS accountiploginaccount01_10count
                    , sum(CASE WHEN AAA.iploginaccountcount >= 11 AND AAA.iploginaccountcount <= 30 THEN 1 ELSE 0 END) AS accountiploginaccount11_30count
                    , sum(CASE WHEN AAA.iploginaccountcount >= 31 AND AAA.iploginaccountcount <= 60 THEN 1 ELSE 0 END) AS accountiploginaccount31_60count
                    , sum(CASE WHEN AAA.iploginaccountcount >= 61 AND AAA.iploginaccountcount <= 100 THEN 1 ELSE 0 END) AS accountiploginaccount61_100upcount
                    , sum(CASE WHEN AAA.iploginaccountcount >= 101 THEN 1 ELSE 0 END) AS accountiploginaccount101upcount
                FROM accountiploginaccountdetail AAA
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'accountiploginaccount'::text as datatype1
                , country::text as datatype2
                , 'accountcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountcount::text as value
            FROM accountiploginaccount
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'accountiploginaccount'::text as datatype1
                , country::text as datatype2
                , 'accountiploginaccount01_10count'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountiploginaccount01_10count::text as value
            FROM accountiploginaccount
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'accountiploginaccount'::text as datatype1
                , country::text as datatype2
                , 'accountiploginaccount11_30count'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountiploginaccount11_30count::text as value
            FROM accountiploginaccount
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'accountiploginaccount'::text as datatype1
                , country::text as datatype2
                , 'accountiploginaccount31_60count'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountiploginaccount31_60count::text as value
            FROM accountiploginaccount
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'accountiploginaccount'::text as datatype1
                , country::text as datatype2
                , 'accountiploginaccount61_100upcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountiploginaccount61_100upcount::text as value
            FROM accountiploginaccount
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'accountiploginaccount'::text as datatype1
                , country::text as datatype2
                , 'accountiploginaccount101upcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountiploginaccount101upcount::text as value
            FROM accountiploginaccount;

            WITH basic_data AS (
                SELECT
                    aa.uniquestr_1 as ip
                    , lower(AA.commondata_7) as country
                    , count(DISTINCT commondata_1) AS accountlogincount
                FROM [:GameName].bu1806 aa
                WHERE 1 = 1
                    and AA.dt >= '[:StartDateLine]'
                    and AA.dt <= '[:EndDateLine]'
                    AND AA.world = 'COMMON'
                GROUP BY
                    aa.uniquestr_1
                    , AA.commondata_7
            ) , ipaccountlogin AS (
                SELECT
                    '[:DataReportName]' as dt
                    , AAA.country
                    , sum(1) AS ipcount
                    , sum(CASE WHEN AAA.accountlogincount >= 1 AND AAA.accountlogincount <= 10 THEN 1 ELSE 0 END) AS accountlogin01_10count
                    , sum(CASE WHEN AAA.accountlogincount >= 11 AND AAA.accountlogincount <= 30 THEN 1 ELSE 0 END) AS accountlogin11_30count
                    , sum(CASE WHEN AAA.accountlogincount >= 31 AND AAA.accountlogincount <= 60 THEN 1 ELSE 0 END) AS accountlogin31_60count
                    , sum(CASE WHEN AAA.accountlogincount >= 61 AND AAA.accountlogincount <= 100 THEN 1 ELSE 0 END) AS accountlogin61_100upcount
                    , sum(CASE WHEN AAA.accountlogincount >= 101 THEN 1 ELSE 0 END) AS accountlogin101upcount
                FROM basic_data AAA
                group by
                    AAA.country
                union all
                SELECT
                    '[:DataReportName]' as dt
                    , 'all' as country
                    , sum(1) AS accountcount
                    , sum(CASE WHEN AAA.accountlogincount >= 1 AND AAA.accountlogincount <= 10 THEN 1 ELSE 0 END) AS accountlogin01_10count
                    , sum(CASE WHEN AAA.accountlogincount >= 11 AND AAA.accountlogincount <= 30 THEN 1 ELSE 0 END) AS accountlogin11_30count
                    , sum(CASE WHEN AAA.accountlogincount >= 31 AND AAA.accountlogincount <= 60 THEN 1 ELSE 0 END) AS accountlogin31_60count
                    , sum(CASE WHEN AAA.accountlogincount >= 61 AND AAA.accountlogincount <= 100 THEN 1 ELSE 0 END) AS accountlogin61_100upcount
                    , sum(CASE WHEN AAA.accountlogincount >= 101 THEN 1 ELSE 0 END) AS accountlogin101upcount
                FROM basic_data AAA
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'ipaccountlogin'::text as datatype1
                , country::text as datatype2
                , 'ipcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , ipcount::text as value
            FROM ipaccountlogin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'ipaccountlogin'::text as datatype1
                , country::text as datatype2
                , 'accountlogin01_10count'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountlogin01_10count::text as value
            FROM ipaccountlogin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'ipaccountlogin'::text as datatype1
                , country::text as datatype2
                , 'accountlogin11_30count'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountlogin11_30count::text as value
            FROM ipaccountlogin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'ipaccountlogin'::text as datatype1
                , country::text as datatype2
                , 'accountlogin31_60count'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountlogin31_60count::text as value
            FROM ipaccountlogin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'ipaccountlogin'::text as datatype1
                , country::text as datatype2
                , 'accountlogin61_100upcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountlogin61_100upcount::text as value
            FROM ipaccountlogin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'ipaccountlogin'::text as datatype1
                , country::text as datatype2
                , 'accountlogin101upcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , accountlogin101upcount::text as value
            FROM ipaccountlogin ;

        """
        return "OrderInsert", [insertSQL]

