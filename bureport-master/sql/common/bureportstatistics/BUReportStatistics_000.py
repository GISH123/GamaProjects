
class BUReportStatistics_000() :

    @classmethod
    def make001DataSQL(self, makeInfo):
        insertSQL = """
            WITH basic_data_login AS (
                SELECT
                    '[:DataReportName]' as dt
                    , count(DISTINCT case when lower(AA.commondata_7) = 'tw' then AA.commondata_1 else null end) AS twcmslogin
                    , count(DISTINCT case when lower(AA.commondata_7) = 'hk' then AA.commondata_1 else null end) AS hkcmslogin
                    , count(DISTINCT AA.commondata_1 ) AS allcmslogin
                FROM [:GameName].bu1001 AA
                WHERE 1 = 1
                    AND AA.dt >= '[:StartDateLine]'
                    AND AA.dt <= '[:EndDateLine]'
                    AND AA.uniqueint_2 != 0
            ) , basic_data_income AS (
                SELECT
                    '[:DataReportName]' as dt
                    , sum(case when lower(AA.commondata_7) = 'tw' then AA.uniqueint_1 else 0 end) AS twcmspoint
                    , sum(case when lower(AA.commondata_7) = 'hk' then AA.uniqueint_1 else 0 end) AS hkcmspoint
                    , sum(AA.uniqueint_1) AS allcmspoint
                FROM [:GameName].bu6002 AA
                where 1 = 1
                    and AA.dt >= '[:StartDateLine]'
                    and AA.dt <= '[:EndDateLine]'
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'cmsdata' as datatype1
                , 'tw' as datatype2
                , 'cmslogin' as datatype3
                , null as datatype4
                , null as datatype5
                , twcmslogin as value
            FROM basic_data_login
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'cmsdata' as datatype1
                , 'all' as datatype2
                , 'cmslogin' as datatype3
                , null as datatype4
                , null as datatype5
                , allcmslogin as value
            FROM basic_data_login
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'cmsdata' as datatype1
                , 'tw' as datatype2
                , 'cmspoint' as datatype3
                , null as datatype4
                , null as datatype5
                , twcmspoint as value
            FROM basic_data_income
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'cmsdata' as datatype1
                , 'all' as datatype2
                , 'cmspoint' as datatype3
                , null as datatype4
                , null as datatype5
                , allcmspoint as value
            FROM basic_data_income ;
        """
        return "OrderInsert", [insertSQL]


