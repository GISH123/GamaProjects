
class BUReportStatistics_100() :

    @classmethod
    def make101DataSQL(self, makeInfo):
        insertSQL = """
            with bflogin_data as (
                select 
                    '[:DataReportName]' as dt
                    , count(distinct AA.commondata_6) as twbfcount
                    , count(distinct AA.commondata_5) as twmaincount
                    , count(distinct AA.commondata_1) as twservicecount
                from [:GameName].bu1001 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as enddate
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'bflogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twbfcount::text as value
            FROM bflogin_data
            union all
            SELECT 
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as enddate
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'bflogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'maincount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twmaincount::text as value
            FROM bflogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as enddate
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'bflogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'servicecount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twservicecount::text as value
            FROM bflogin_data ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make102DataSQL(self, makeInfo):
        insertSQL = """
           with accountlogin_data as (
               select 
                   '[:DataReportName]' as dt
                   , count(distinct AA.commondata_6) as twbfcount
                   , count(distinct AA.commondata_5) as twmaincount
                   , count(distinct case when AA.commondata_5 is not null then AA.commondata_1 else null end ) as twservicecount
                   , count(distinct case when AA.commondata_5 is not null then AA.commondata_2 else null end ) as twaccountcount
                   , count(distinct case when AA.commondata_5 is null then AA.commondata_1 else null end ) as hkservicecount
                   , count(distinct case when AA.commondata_5 is null then AA.commondata_2 else null end ) as hkaccountcount
                   , count(distinct AA.commondata_1) as allservicecount
                   , count(distinct AA.commondata_2) as allaccountcount
               from [:GameName].bu1002 AA
               WHERE 1 = 1
                   and aa.dt >= '[:StartDateLine]'
                   and aa.dt <= '[:EndDateLine]'
           ) INSERT INTO [:GameName].bureportstatistics
           SELECT
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'tw'::text as datatype2
               , 'bfcount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , twbfcount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'tw'::text as datatype2
               , 'maincount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , twmaincount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'tw'::text as datatype2
               , 'servicecount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , twservicecount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'tw'::text as datatype2
               , 'accountcount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , twaccountcount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'hk'::text as datatype2
               , 'servicecount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , hkservicecount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'hk'::text as datatype2
               , 'accountcount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , hkaccountcount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'all'::text as datatype2
               , 'servicecount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , allservicecount::text as value
           FROM accountlogin_data
           union all
           SELECT 
               dt::text as reportName
               , '[:StartDateLine]'::date as startdate
               , '[:EndDateLine]'::date as enddate
               , '[:PeriodType]' as periodType
               , '[:GameName]'::text as gamename
               , '[:ReportCode]'::text as reportcode
               , 'accountlogin'::text as datatype1
               , 'all'::text as datatype2
               , 'accountcount'::text as datatype3
               , null::text as datatype4
               , null::text as datatype5
               , allaccountcount::text as value
           FROM accountlogin_data ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make103DataSQL(self, makeInfo):
        insertSQL = """
            with charlogin_data as (
                select
                    '[:DataReportName]' as dt
                    , count(distinct AA.commondata_6) as twbfcount
                    , count(distinct AA.commondata_5) as twmaincount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_1 else null end ) as twservicecount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_2 else null end ) as twaccountcount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_3 else null end ) as twcharcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_1 else null end ) as hkservicecount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_2 else null end ) as hkaccountcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_3 else null end ) as hkcharcount
                    , count(distinct AA.commondata_1) as allservicecount
                    , count(distinct AA.commondata_2) as allaccountcount
                    , count(distinct AA.commondata_3) as allcharcount
                from [:GameName].bu1003 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as enddate
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twbfcount::text as value
            FROM charlogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'maincount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twmaincount::text as value
            FROM charlogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'servicecount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twservicecount::text as value
            FROM charlogin_data ;
            
            with charlogin_data as (
                select
                    '[:DataReportName]' as dt
                    , count(distinct AA.commondata_6) as twbfcount
                    , count(distinct AA.commondata_5) as twmaincount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_1 else null end ) as twservicecount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_2 else null end ) as twaccountcount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_3 else null end ) as twcharcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_1 else null end ) as hkservicecount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_2 else null end ) as hkaccountcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_3 else null end ) as hkcharcount
                    , count(distinct AA.commondata_1) as allservicecount
                    , count(distinct AA.commondata_2) as allaccountcount
                    , count(distinct AA.commondata_3) as allcharcount
                from [:GameName].bu1003 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'accountcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twaccountcount::text as value
            FROM charlogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'hk'::text as datatype2
                , 'servicecount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hkservicecount::text as value
            FROM charlogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'hk'::text as datatype2
                , 'accountcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hkaccountcount::text as value
            FROM charlogin_data ;
            
            with charlogin_data as (
                select
                    '[:DataReportName]' as dt
                    , count(distinct AA.commondata_6) as twbfcount
                    , count(distinct AA.commondata_5) as twmaincount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_1 else null end ) as twservicecount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_2 else null end ) as twaccountcount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_3 else null end ) as twcharcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_1 else null end ) as hkservicecount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_2 else null end ) as hkaccountcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_3 else null end ) as hkcharcount
                    , count(distinct AA.commondata_1) as allservicecount
                    , count(distinct AA.commondata_2) as allaccountcount
                    , count(distinct AA.commondata_3) as allcharcount
                from [:GameName].bu1003 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'charcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , twcharcount::text as value
            FROM charlogin_data
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'hk'::text as datatype2
                , 'charcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hkcharcount::text as value
            FROM charlogin_data ;
            
            with charlogin_data as (
                select
                    '[:DataReportName]' as dt
                    , count(distinct AA.commondata_6) as twbfcount
                    , count(distinct AA.commondata_5) as twmaincount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_1 else null end ) as twservicecount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_2 else null end ) as twaccountcount
                    , count(distinct case when AA.commondata_5 is not null then AA.commondata_3 else null end ) as twcharcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_1 else null end ) as hkservicecount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_2 else null end ) as hkaccountcount
                    , count(distinct case when AA.commondata_5 is null then AA.commondata_3 else null end ) as hkcharcount
                    , count(distinct AA.commondata_1) as allservicecount
                    , count(distinct AA.commondata_2) as allaccountcount
                    , count(distinct AA.commondata_3) as allcharcount
                from [:GameName].bu1003 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'all'::text as datatype2
                , 'servicecount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , allservicecount::text as value
            FROM charlogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'all'::text as datatype2
                , 'accountcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , allaccountcount::text as value
            FROM charlogin_data
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'charlogin'::text as datatype1
                , 'all'::text as datatype2
                , 'charcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , allcharcount::text as value
            FROM charlogin_data ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make106DataSQL(self, makeInfo):
        insertSQL = """
            with checklogin_data as (
                select
                    AAA.dt
                    , AAA.serviceaccountid
                    , MAX(case when logintype = 'bflogin' then 1 else 0 end ) as bflogin
                    , MAX(case when logintype = 'accountlogin' then 1 else 0 end ) as accountlogin
                    , MAX(case when logintype = 'characterlogin' then 1 else 0 end ) as characterlogin
                from (
                    select
                        '[:DataReportName]' as dt
                        , 'bflogin' as logintype
                        , lower(AA.commondata_1) as serviceaccountid
                    from [:GameName].bu1001 AA
                    where 1 = 1
                        and AA.commondata_5 is not null
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    group by
                        AA.dt
                        , lower(AA.commondata_1)
                    union all
                    select
                        '[:DataReportName]' as dt
                        , 'accountlogin'
                        , lower(AA.commondata_1) as serviceaccountid
                    from [:GameName].bu1002 AA
                    where 1 = 1
                        and AA.commondata_5 is not null
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    group by
                        AA.dt
                        , lower(AA.commondata_1)
                    union all
                    select
                        '[:DataReportName]' as dt
                        , 'characterlogin'
                        , lower(AA.commondata_1) as serviceaccountid
                    from [:GameName].bu1003 AA
                    where 1 = 1
                        and AA.commondata_5 is not null
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    group by
                        AA.dt
                        , lower(AA.commondata_1)
                ) AAA
                group by
                    AAA.dt
                    , AAA.serviceaccountid
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfx_gax_cho'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 0
                and accountlogin = 0
                and characterlogin = 1
            group by
                dt
            union all
            select
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfx_gao_chx'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 0
                and accountlogin = 1
                and characterlogin = 0
            group by
                dt
            union all
            select
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfx_gao_cho'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 0
                and accountlogin = 1
                and characterlogin = 1
            group by
                dt
            union all
            select
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfo_gax_chx'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 1
                and accountlogin = 0
                and characterlogin = 0
            group by
                dt
            union all
            select
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfo_gax_cho'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 1
                and accountlogin = 0
                and characterlogin = 1
            group by
                dt
            union all
            select
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfo_gao_chx'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 1
                and accountlogin = 1
                and characterlogin = 0
            group by
                dt
            union all
            select
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'checklogin'::text as datatype1
                , 'tw'::text as datatype2
                , 'bfo_gao_cho'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , count(distinct serviceaccountid)::text as value
            FROM checklogin_data
            where 1 = 1
                and bflogin = 1
                and accountlogin = 1
                and characterlogin = 1
            group by
                dt ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make107DataSQL(self, makeInfo):
        insertSQL = """
            WITH basic_data AS (
                SELECT
                    AAA.serviceid
                    , MAX(AAA.country) AS country
                    , MAX(CASE WHEN AAA.dataname = '1003' AND AAA.dt >= '[:StartDateLine]' AND AAA.dt <= '[:EndDateLine]' THEN 1 ELSE 0 END ) AS NOWLOGIN
                    , MAX(CASE WHEN AAA.dataname = '1003' AND AAA.dt >= '[:PreStartDateLine]' AND AAA.dt <= '[:PreEndDateLine]' THEN 1 ELSE 0 END ) AS PRELOGIN
                    , MAX(CASE WHEN AAA.dataname = '1002' AND AAA.createtime >= '[:StartDateLine]' AND AAA.createtime <= '[:EndDateLine]' THEN 1 ELSE 0 END ) AS NEWLOGIN
                FROM (
                    SELECT
                        DISTINCT '1003' AS dataname
                        , AA.dt  AS dt
                        , AA.commondata_1 AS serviceid
                        , lower(AA.commondata_7) as country
                        , NULL AS createtime
                    FROM [:GameName].bu1003 AA
                    WHERE 1 = 1
                        AND ( 1 != 1
                            OR (AA.dt >= '[:StartDateLine]' AND AA.dt <= '[:EndDateLine]')
                            OR (AA.dt >= '[:PreStartDateLine]' AND AA.dt <= '[:PreEndDateLine]')
                        )
                    UNION ALL
                    SELECT
                        DISTINCT '1002' AS dataname
                        , AA.dt  AS dt
                        , AA.commondata_1 AS serviceid
                        , lower(AA.commondata_7) as country
                        , to_char(AA.uniquetime_1,'YYYY-MM-DD') AS createtime
                    FROM [:GameName].bu1002 AA
                    WHERE 1 = 1
                        AND ( 1 != 1
                            OR (AA.dt >= '[:StartDateLine]' AND AA.dt <= '[:EndDateLine]')
                            OR (AA.dt >= '[:PreStartDateLine]' AND AA.dt <= '[:PreEndDateLine]')
                        )
                ) AAA
                GROUP BY
                    AAA.serviceid
            ) , stepstatedata AS (
                SELECT
                    '[:DataReportName]' as dt
                    , AAAA.country
                    , SUM(CASE WHEN AAAA.nowlogin = 1 AND AAAA.prelogin = 1 THEN 1 ELSE 0 END) AS step
                    , SUM(CASE WHEN AAAA.nowlogin = 1 AND AAAA.prelogin = 0 AND AAAA.newlogin = 0 THEN 1 ELSE 0 END) AS reback
                    , SUM(CASE WHEN AAAA.nowlogin = 1 AND AAAA.prelogin = 0 AND AAAA.newlogin = 1 THEN 1 ELSE 0 END) AS newcount
                    , SUM(CASE WHEN AAAA.nowlogin = 0 AND AAAA.prelogin = 1 THEN 1 ELSE 0 END) AS loss
                FROM basic_data AAAA
                GROUP BY
                    AAAA.country
                UNION aLL
                SELECT
                    '[:DataReportName]' as dt
                    , 'all' AS country
                    , SUM(CASE WHEN AAAA.nowlogin = 1 AND AAAA.prelogin = 1 THEN 1 ELSE 0 END) AS step
                    , SUM(CASE WHEN AAAA.nowlogin = 1 AND AAAA.prelogin = 0 AND AAAA.newlogin = 0 THEN 1 ELSE 0 END) AS reback
                    , SUM(CASE WHEN AAAA.nowlogin = 1 AND AAAA.prelogin = 0 AND AAAA.newlogin = 1 THEN 1 ELSE 0 END) AS newcount
                    , SUM(CASE WHEN AAAA.nowlogin = 0 AND AAAA.prelogin = 1 THEN 1 ELSE 0 END) AS loss
                FROM basic_data AAAA
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'stepstate'::text as datatype1
                , country::text as datatype2
                , 'step'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , step::text as value
            FROM stepstatedata
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'stepstate'::text as datatype1
                , country::text as datatype2
                , 'reback'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , reback::text as value
            FROM stepstatedata
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'stepstate'::text as datatype1
                , country::text as datatype2
                , 'newcount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , newcount::text as value
            FROM stepstatedata
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'stepstate'::text as datatype1
                , country::text as datatype2
                , 'loss'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , loss::text as value
            FROM stepstatedata ;

        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make108DataSQL(self, makeInfo):
        insertSQL = """
            WITH basic_data AS (
                SELECT
                    aa.commondata_1 as serviceaccountid
                    , lower(AA.commondata_7) as country
                    , AA.uniquestr_1 AS ip
                FROM [:GameName].bu1804 aa
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
                FROM [:GameName].bu1804 aa
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

    @classmethod
    def make111DataSQL(self, makeInfo):
        insertSQL = """
            with basic_data as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as sec00_01
                    , AA.UniqueInt_2 as sec01_02
                    , AA.UniqueInt_3 as sec02_03
                    , AA.UniqueInt_4 as sec03_04
                    , AA.UniqueInt_5 as sec04_05
                    , AA.UniqueInt_6 as sec05_06
                    , AA.UniqueInt_7 as sec06_07
                    , AA.UniqueInt_8 as sec07_08
                    , AA.UniqueInt_9 as sec08_09
                    , AA.UniqueInt_10 as sec09_10
                    , AA.UniqueInt_11 as sec10_11
                    , AA.UniqueInt_12 as sec11_12
                    , 0 as  sec12_13 --
                    , 0 as  sec13_14 --
                    , 0 as  sec14_15 --
                    , 0 as  sec15_16 --
                    , 0 as  sec16_17 --
                    , 0 as  sec17_18 --
                    , 0 as  sec18_19 --
                    , 0 as  sec19_20 --
                    , 0 as  sec20_21 --
                    , 0 as  sec21_22 --
                    , 0 as  sec22_23 --
                    , 0 as  sec23_24 --
                    , 0 as  secday --
                FROM [:GameName].bu1132 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , 0 as  sec00_01 --
                    , 0 as  sec01_02 --
                    , 0 as  sec02_03 --
                    , 0 as  sec03_04 --
                    , 0 as  sec04_05 --
                    , 0 as  sec05_06 --
                    , 0 as  sec06_07 --
                    , 0 as  sec07_08 --
                    , 0 as  sec08_09 --
                    , 0 as  sec09_10 --
                    , 0 as  sec10_11 --
                    , 0 as  sec11_12 --
                    , UniqueInt_1 as sec12_13 --
                    , UniqueInt_2 as sec13_14 --
                    , UniqueInt_3 as sec14_15 --
                    , UniqueInt_4 as sec15_16 --
                    , UniqueInt_5 as sec16_17 --
                    , UniqueInt_6 as sec17_18 --
                    , UniqueInt_7 as sec18_19 --
                    , UniqueInt_8 as sec19_20 --
                    , UniqueInt_9 as sec20_21 --
                    , UniqueInt_10 as sec21_22 --
                    , UniqueInt_11 as sec22_23 --
                    , UniqueInt_12 as sec23_24 --
                    , UniqueInt_15 as secday --
                FROM [:GameName].bu1133 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), logincountbyaccountid as (
                SELECT
                    AA.dt
                    , AA.accountid
                    , lower(MAX(AA.country)) as country
                    , SUM(AA.sec00_01) as sec00_01
                    , SUM(AA.sec01_02) as sec01_02
                    , SUM(AA.sec02_03) as sec02_03
                    , SUM(AA.sec03_04) as sec03_04
                    , SUM(AA.sec04_05) as sec04_05
                    , SUM(AA.sec05_06) as sec05_06
                    , SUM(AA.sec06_07) as sec06_07
                    , SUM(AA.sec07_08) as sec07_08
                    , SUM(AA.sec08_09) as sec08_09
                    , SUM(AA.sec09_10) as sec09_10
                    , SUM(AA.sec10_11) as sec10_11
                    , SUM(AA.sec11_12) as sec11_12
                    , SUM(AA.sec12_13) as sec12_13
                    , SUM(AA.sec13_14) as sec13_14
                    , SUM(AA.sec14_15) as sec14_15
                    , SUM(AA.sec15_16) as sec15_16
                    , SUM(AA.sec16_17) as sec16_17
                    , SUM(AA.sec17_18) as sec17_18
                    , SUM(AA.sec18_19) as sec18_19
                    , SUM(AA.sec19_20) as sec19_20
                    , SUM(AA.sec20_21) as sec20_21
                    , SUM(AA.sec21_22) as sec21_22
                    , SUM(AA.sec22_23) as sec22_23
                    , SUM(AA.sec23_24) as sec23_24
                    , SUM(AA.secday) as secday
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.accountid
            ), logincountbygroupmin as (
                SELECT
                    AAA.dt
                    , 'all' as country
                    , SUM(CASE WHEN AAA.secday <= 3600 THEN 1 ELSE 0 END ) as min60_logincount
                    , SUM(CASE WHEN AAA.secday > 3600 and AAA.secday <= 10800 THEN 1 ELSE 0 END ) as min180_logincount
                    , SUM(CASE WHEN AAA.secday > 10800 and AAA.secday <= 21600 THEN 1 ELSE 0 END ) as min360_logincount
                    , SUM(CASE WHEN AAA.secday > 21600 THEN 1 ELSE 0 END ) as min361_logincount
                    , MAX(AAA.secday)
                FROM logincountbyaccountid AAA
                GROUP BY
                    AAA.dt
                UNION ALL
                SELECT
                    AAA.dt
                    , AAA.country as country
                    , SUM(CASE WHEN AAA.secday <= 3600 THEN 1 ELSE 0 END ) as min60_logincount
                    , SUM(CASE WHEN AAA.secday > 3600 and AAA.secday <= 10800 THEN 1 ELSE 0 END ) as min180_logincount
                    , SUM(CASE WHEN AAA.secday > 10800 and AAA.secday <= 21600 THEN 1 ELSE 0 END ) as min360_logincount
                    , SUM(CASE WHEN AAA.secday > 21600 THEN 1 ELSE 0 END ) as min361_logincount
                    , MAX(AAA.secday)
                FROM logincountbyaccountid AAA
                GROUP BY
                    AAA.dt
                    , AAA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text::text as reportcode
                , 'logincountbygroupmin'::text as datatype1
                , country::text as datatype2
                , 'min60_logincount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , min60_logincount::text as value
            FROM logincountbygroupmin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text::text as reportcode
                , 'logincountbygroupmin'::text as datatype1
                , country::text as datatype2
                , 'min180_logincount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , min180_logincount::text as value
            FROM logincountbygroupmin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text::text as reportcode
                , 'logincountbygroupmin'::text as datatype1
                , country::text as datatype2
                , 'min360_logincount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , min360_logincount::text as value
            FROM logincountbygroupmin
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text::text as reportcode
                , 'logincountbygroupmin'::text as datatype1
                , country::text as datatype2
                , 'min361_logincount'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , min361_logincount::text as value
            FROM logincountbygroupmin ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make112DataSQL(self, makeInfo):
        insertSQL = """
            with basic_data as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as sec00_01
                    , AA.UniqueInt_2 as sec01_02
                    , AA.UniqueInt_3 as sec02_03
                    , AA.UniqueInt_4 as sec03_04
                    , AA.UniqueInt_5 as sec04_05
                    , AA.UniqueInt_6 as sec05_06
                    , AA.UniqueInt_7 as sec06_07
                    , AA.UniqueInt_8 as sec07_08
                    , AA.UniqueInt_9 as sec08_09
                    , AA.UniqueInt_10 as sec09_10
                    , AA.UniqueInt_11 as sec10_11
                    , AA.UniqueInt_12 as sec11_12
                    , 0 as  sec12_13 --
                    , 0 as  sec13_14 --
                    , 0 as  sec14_15 --
                    , 0 as  sec15_16 --
                    , 0 as  sec16_17 --
                    , 0 as  sec17_18 --
                    , 0 as  sec18_19 --
                    , 0 as  sec19_20 --
                    , 0 as  sec20_21 --
                    , 0 as  sec21_22 --
                    , 0 as  sec22_23 --
                    , 0 as  sec23_24 --
                    , 0 as  secday --
                FROM [:GameName].bu1132 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , 0 as  sec00_01 --
                    , 0 as  sec01_02 --
                    , 0 as  sec02_03 --
                    , 0 as  sec03_04 --
                    , 0 as  sec04_05 --
                    , 0 as  sec05_06 --
                    , 0 as  sec06_07 --
                    , 0 as  sec07_08 --
                    , 0 as  sec08_09 --
                    , 0 as  sec09_10 --
                    , 0 as  sec10_11 --
                    , 0 as  sec11_12 --
                    , UniqueInt_1 as sec12_13 --
                    , UniqueInt_2 as sec13_14 --
                    , UniqueInt_3 as sec14_15 --
                    , UniqueInt_4 as sec15_16 --
                    , UniqueInt_5 as sec16_17 --
                    , UniqueInt_6 as sec17_18 --
                    , UniqueInt_7 as sec18_19 --
                    , UniqueInt_8 as sec19_20 --
                    , UniqueInt_9 as sec20_21 --
                    , UniqueInt_10 as sec21_22 --
                    , UniqueInt_11 as sec22_23 --
                    , UniqueInt_12 as sec23_24 --
                    , UniqueInt_15 as secday --
                FROM [:GameName].bu1133 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), logincountbyaccountid as (
                SELECT
                    AA.dt
                    , AA.accountid
                    , lower(MAX(AA.country)) as country
                    , SUM(AA.sec00_01) as sec00_01
                    , SUM(AA.sec01_02) as sec01_02
                    , SUM(AA.sec02_03) as sec02_03
                    , SUM(AA.sec03_04) as sec03_04
                    , SUM(AA.sec04_05) as sec04_05
                    , SUM(AA.sec05_06) as sec05_06
                    , SUM(AA.sec06_07) as sec06_07
                    , SUM(AA.sec07_08) as sec07_08
                    , SUM(AA.sec08_09) as sec08_09
                    , SUM(AA.sec09_10) as sec09_10
                    , SUM(AA.sec10_11) as sec10_11
                    , SUM(AA.sec11_12) as sec11_12
                    , SUM(AA.sec12_13) as sec12_13
                    , SUM(AA.sec13_14) as sec13_14
                    , SUM(AA.sec14_15) as sec14_15
                    , SUM(AA.sec15_16) as sec15_16
                    , SUM(AA.sec16_17) as sec16_17
                    , SUM(AA.sec17_18) as sec17_18
                    , SUM(AA.sec18_19) as sec18_19
                    , SUM(AA.sec19_20) as sec19_20
                    , SUM(AA.sec20_21) as sec20_21
                    , SUM(AA.sec21_22) as sec21_22
                    , SUM(AA.sec22_23) as sec22_23
                    , SUM(AA.sec23_24) as sec23_24
                    , SUM(AA.secday) as secday
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.accountid
            ), logincountbyeveryhour as (
                SELECT
                    AAA.dt
                    , AAA.country as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                WHERE 1 = 1
                GROUP BY
                    AAA.dt
                    , AAA.country
                UNION ALL
                SELECT
                    AAA.dt
                    , 'all' as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                GROUP BY
                    AAA.dt
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour00_01'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec00_01_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour01_02'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec01_02_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour02_03'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec02_03_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour03_04'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec03_04_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour04_05'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec04_05_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour05_06'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec05_06_logincount::text as value
            FROM logincountbyeveryhour ;
            
            with basic_data as (
                    SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as sec00_01
                    , AA.UniqueInt_2 as sec01_02
                    , AA.UniqueInt_3 as sec02_03
                    , AA.UniqueInt_4 as sec03_04
                    , AA.UniqueInt_5 as sec04_05
                    , AA.UniqueInt_6 as sec05_06
                    , AA.UniqueInt_7 as sec06_07
                    , AA.UniqueInt_8 as sec07_08
                    , AA.UniqueInt_9 as sec08_09
                    , AA.UniqueInt_10 as sec09_10
                    , AA.UniqueInt_11 as sec10_11
                    , AA.UniqueInt_12 as sec11_12
                    , 0 as  sec12_13 --
                    , 0 as  sec13_14 --
                    , 0 as  sec14_15 --
                    , 0 as  sec15_16 --
                    , 0 as  sec16_17 --
                    , 0 as  sec17_18 --
                    , 0 as  sec18_19 --
                    , 0 as  sec19_20 --
                    , 0 as  sec20_21 --
                    , 0 as  sec21_22 --
                    , 0 as  sec22_23 --
                    , 0 as  sec23_24 --
                    , 0 as  secday --
                FROM [:GameName].bu1132 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , 0 as  sec00_01 --
                    , 0 as  sec01_02 --
                    , 0 as  sec02_03 --
                    , 0 as  sec03_04 --
                    , 0 as  sec04_05 --
                    , 0 as  sec05_06 --
                    , 0 as  sec06_07 --
                    , 0 as  sec07_08 --
                    , 0 as  sec08_09 --
                    , 0 as  sec09_10 --
                    , 0 as  sec10_11 --
                    , 0 as  sec11_12 --
                    , UniqueInt_1 as sec12_13 --
                    , UniqueInt_2 as sec13_14 --
                    , UniqueInt_3 as sec14_15 --
                    , UniqueInt_4 as sec15_16 --
                    , UniqueInt_5 as sec16_17 --
                    , UniqueInt_6 as sec17_18 --
                    , UniqueInt_7 as sec18_19 --
                    , UniqueInt_8 as sec19_20 --
                    , UniqueInt_9 as sec20_21 --
                    , UniqueInt_10 as sec21_22 --
                    , UniqueInt_11 as sec22_23 --
                    , UniqueInt_12 as sec23_24 --
                    , UniqueInt_15 as secday --
                FROM [:GameName].bu1133 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), logincountbyaccountid as (
                SELECT
                    AA.dt
                    , AA.accountid
                    , lower(MAX(AA.country)) as country
                    , SUM(AA.sec00_01) as sec00_01
                    , SUM(AA.sec01_02) as sec01_02
                    , SUM(AA.sec02_03) as sec02_03
                    , SUM(AA.sec03_04) as sec03_04
                    , SUM(AA.sec04_05) as sec04_05
                    , SUM(AA.sec05_06) as sec05_06
                    , SUM(AA.sec06_07) as sec06_07
                    , SUM(AA.sec07_08) as sec07_08
                    , SUM(AA.sec08_09) as sec08_09
                    , SUM(AA.sec09_10) as sec09_10
                    , SUM(AA.sec10_11) as sec10_11
                    , SUM(AA.sec11_12) as sec11_12
                    , SUM(AA.sec12_13) as sec12_13
                    , SUM(AA.sec13_14) as sec13_14
                    , SUM(AA.sec14_15) as sec14_15
                    , SUM(AA.sec15_16) as sec15_16
                    , SUM(AA.sec16_17) as sec16_17
                    , SUM(AA.sec17_18) as sec17_18
                    , SUM(AA.sec18_19) as sec18_19
                    , SUM(AA.sec19_20) as sec19_20
                    , SUM(AA.sec20_21) as sec20_21
                    , SUM(AA.sec21_22) as sec21_22
                    , SUM(AA.sec22_23) as sec22_23
                    , SUM(AA.sec23_24) as sec23_24
                    , SUM(AA.secday) as secday
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.accountid
            ), logincountbyeveryhour as (
                SELECT
                    AAA.dt
                    , AAA.country as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                WHERE 1 = 1
                GROUP BY
                    AAA.dt
                    , AAA.country
                UNION ALL
                SELECT
                    AAA.dt
                    , 'all' as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                GROUP BY
                    AAA.dt
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour06_07'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec06_07_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour07_08'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec07_08_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour08_09'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec08_09_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour09_10'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec09_10_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour10_11'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec10_11_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour11_12'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec11_12_logincount::text as value
            FROM logincountbyeveryhour;
            
            
            with basic_data as (
                    SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as sec00_01
                    , AA.UniqueInt_2 as sec01_02
                    , AA.UniqueInt_3 as sec02_03
                    , AA.UniqueInt_4 as sec03_04
                    , AA.UniqueInt_5 as sec04_05
                    , AA.UniqueInt_6 as sec05_06
                    , AA.UniqueInt_7 as sec06_07
                    , AA.UniqueInt_8 as sec07_08
                    , AA.UniqueInt_9 as sec08_09
                    , AA.UniqueInt_10 as sec09_10
                    , AA.UniqueInt_11 as sec10_11
                    , AA.UniqueInt_12 as sec11_12
                    , 0 as  sec12_13 --
                    , 0 as  sec13_14 --
                    , 0 as  sec14_15 --
                    , 0 as  sec15_16 --
                    , 0 as  sec16_17 --
                    , 0 as  sec17_18 --
                    , 0 as  sec18_19 --
                    , 0 as  sec19_20 --
                    , 0 as  sec20_21 --
                    , 0 as  sec21_22 --
                    , 0 as  sec22_23 --
                    , 0 as  sec23_24 --
                    , 0 as  secday --
                FROM [:GameName].bu1132 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , 0 as  sec00_01 --
                    , 0 as  sec01_02 --
                    , 0 as  sec02_03 --
                    , 0 as  sec03_04 --
                    , 0 as  sec04_05 --
                    , 0 as  sec05_06 --
                    , 0 as  sec06_07 --
                    , 0 as  sec07_08 --
                    , 0 as  sec08_09 --
                    , 0 as  sec09_10 --
                    , 0 as  sec10_11 --
                    , 0 as  sec11_12 --
                    , UniqueInt_1 as sec12_13 --
                    , UniqueInt_2 as sec13_14 --
                    , UniqueInt_3 as sec14_15 --
                    , UniqueInt_4 as sec15_16 --
                    , UniqueInt_5 as sec16_17 --
                    , UniqueInt_6 as sec17_18 --
                    , UniqueInt_7 as sec18_19 --
                    , UniqueInt_8 as sec19_20 --
                    , UniqueInt_9 as sec20_21 --
                    , UniqueInt_10 as sec21_22 --
                    , UniqueInt_11 as sec22_23 --
                    , UniqueInt_12 as sec23_24 --
                    , UniqueInt_15 as secday --
                FROM [:GameName].bu1133 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), logincountbyaccountid as (
                SELECT
                    AA.dt
                    , AA.accountid
                    , lower(MAX(AA.country)) as country
                    , SUM(AA.sec00_01) as sec00_01
                    , SUM(AA.sec01_02) as sec01_02
                    , SUM(AA.sec02_03) as sec02_03
                    , SUM(AA.sec03_04) as sec03_04
                    , SUM(AA.sec04_05) as sec04_05
                    , SUM(AA.sec05_06) as sec05_06
                    , SUM(AA.sec06_07) as sec06_07
                    , SUM(AA.sec07_08) as sec07_08
                    , SUM(AA.sec08_09) as sec08_09
                    , SUM(AA.sec09_10) as sec09_10
                    , SUM(AA.sec10_11) as sec10_11
                    , SUM(AA.sec11_12) as sec11_12
                    , SUM(AA.sec12_13) as sec12_13
                    , SUM(AA.sec13_14) as sec13_14
                    , SUM(AA.sec14_15) as sec14_15
                    , SUM(AA.sec15_16) as sec15_16
                    , SUM(AA.sec16_17) as sec16_17
                    , SUM(AA.sec17_18) as sec17_18
                    , SUM(AA.sec18_19) as sec18_19
                    , SUM(AA.sec19_20) as sec19_20
                    , SUM(AA.sec20_21) as sec20_21
                    , SUM(AA.sec21_22) as sec21_22
                    , SUM(AA.sec22_23) as sec22_23
                    , SUM(AA.sec23_24) as sec23_24
                    , SUM(AA.secday) as secday
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.accountid
            ), logincountbyeveryhour as (
                SELECT
                    AAA.dt
                    , AAA.country as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                WHERE 1 = 1
                GROUP BY
                    AAA.dt
                    , AAA.country
                UNION ALL
                SELECT
                    AAA.dt
                    , 'all' as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                GROUP BY
                    AAA.dt
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour12_13'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec12_13_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour13_14'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec13_14_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour14_15'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec14_15_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour15_16'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec15_16_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour16_17'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec16_17_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour17_18'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec17_18_logincount::text as value
            FROM logincountbyeveryhour;
            
            
            with basic_data as (
                    SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as sec00_01
                    , AA.UniqueInt_2 as sec01_02
                    , AA.UniqueInt_3 as sec02_03
                    , AA.UniqueInt_4 as sec03_04
                    , AA.UniqueInt_5 as sec04_05
                    , AA.UniqueInt_6 as sec05_06
                    , AA.UniqueInt_7 as sec06_07
                    , AA.UniqueInt_8 as sec07_08
                    , AA.UniqueInt_9 as sec08_09
                    , AA.UniqueInt_10 as sec09_10
                    , AA.UniqueInt_11 as sec10_11
                    , AA.UniqueInt_12 as sec11_12
                    , 0 as  sec12_13 --
                    , 0 as  sec13_14 --
                    , 0 as  sec14_15 --
                    , 0 as  sec15_16 --
                    , 0 as  sec16_17 --
                    , 0 as  sec17_18 --
                    , 0 as  sec18_19 --
                    , 0 as  sec19_20 --
                    , 0 as  sec20_21 --
                    , 0 as  sec21_22 --
                    , 0 as  sec22_23 --
                    , 0 as  sec23_24 --
                    , 0 as  secday --
                FROM [:GameName].bu1132 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , AA.world as world
                    , AA.commondata_3 as characterid
                    , AA.commondata_2 as accountid
                    , AA.commondata_1 as serviceaccountid
                    , AA.commondata_5 as mainaccountid
                    , lower(AA.commondata_7) as country
                    , 0 as  sec00_01 --
                    , 0 as  sec01_02 --
                    , 0 as  sec02_03 --
                    , 0 as  sec03_04 --
                    , 0 as  sec04_05 --
                    , 0 as  sec05_06 --
                    , 0 as  sec06_07 --
                    , 0 as  sec07_08 --
                    , 0 as  sec08_09 --
                    , 0 as  sec09_10 --
                    , 0 as  sec10_11 --
                    , 0 as  sec11_12 --
                    , UniqueInt_1 as sec12_13 --
                    , UniqueInt_2 as sec13_14 --
                    , UniqueInt_3 as sec14_15 --
                    , UniqueInt_4 as sec15_16 --
                    , UniqueInt_5 as sec16_17 --
                    , UniqueInt_6 as sec17_18 --
                    , UniqueInt_7 as sec18_19 --
                    , UniqueInt_8 as sec19_20 --
                    , UniqueInt_9 as sec20_21 --
                    , UniqueInt_10 as sec21_22 --
                    , UniqueInt_11 as sec22_23 --
                    , UniqueInt_12 as sec23_24 --
                    , UniqueInt_15 as secday --
                FROM [:GameName].bu1133 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), logincountbyaccountid as (
                SELECT
                    AA.dt
                    , AA.accountid
                    , lower(MAX(AA.country)) as country
                    , SUM(AA.sec00_01) as sec00_01
                    , SUM(AA.sec01_02) as sec01_02
                    , SUM(AA.sec02_03) as sec02_03
                    , SUM(AA.sec03_04) as sec03_04
                    , SUM(AA.sec04_05) as sec04_05
                    , SUM(AA.sec05_06) as sec05_06
                    , SUM(AA.sec06_07) as sec06_07
                    , SUM(AA.sec07_08) as sec07_08
                    , SUM(AA.sec08_09) as sec08_09
                    , SUM(AA.sec09_10) as sec09_10
                    , SUM(AA.sec10_11) as sec10_11
                    , SUM(AA.sec11_12) as sec11_12
                    , SUM(AA.sec12_13) as sec12_13
                    , SUM(AA.sec13_14) as sec13_14
                    , SUM(AA.sec14_15) as sec14_15
                    , SUM(AA.sec15_16) as sec15_16
                    , SUM(AA.sec16_17) as sec16_17
                    , SUM(AA.sec17_18) as sec17_18
                    , SUM(AA.sec18_19) as sec18_19
                    , SUM(AA.sec19_20) as sec19_20
                    , SUM(AA.sec20_21) as sec20_21
                    , SUM(AA.sec21_22) as sec21_22
                    , SUM(AA.sec22_23) as sec22_23
                    , SUM(AA.sec23_24) as sec23_24
                    , SUM(AA.secday) as secday
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.accountid
            ), logincountbyeveryhour as (
                SELECT
                    AAA.dt
                    , AAA.country as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                WHERE 1 = 1
                GROUP BY
                    AAA.dt
                    , AAA.country
                UNION ALL
                SELECT
                    AAA.dt
                    , 'all' as country
                    , SUM(CASE WHEN AAA.sec00_01 != 0 THEN 1 ELSE 0 END ) as sec00_01_logincount
                    , SUM(CASE WHEN AAA.sec01_02 != 0 THEN 1 ELSE 0 END ) as sec01_02_logincount
                    , SUM(CASE WHEN AAA.sec02_03 != 0 THEN 1 ELSE 0 END ) as sec02_03_logincount
                    , SUM(CASE WHEN AAA.sec03_04 != 0 THEN 1 ELSE 0 END ) as sec03_04_logincount
                    , SUM(CASE WHEN AAA.sec04_05 != 0 THEN 1 ELSE 0 END ) as sec04_05_logincount
                    , SUM(CASE WHEN AAA.sec05_06 != 0 THEN 1 ELSE 0 END ) as sec05_06_logincount
                    , SUM(CASE WHEN AAA.sec06_07 != 0 THEN 1 ELSE 0 END ) as sec06_07_logincount
                    , SUM(CASE WHEN AAA.sec07_08 != 0 THEN 1 ELSE 0 END ) as sec07_08_logincount
                    , SUM(CASE WHEN AAA.sec08_09 != 0 THEN 1 ELSE 0 END ) as sec08_09_logincount
                    , SUM(CASE WHEN AAA.sec09_10 != 0 THEN 1 ELSE 0 END ) as sec09_10_logincount
                    , SUM(CASE WHEN AAA.sec10_11 != 0 THEN 1 ELSE 0 END ) as sec10_11_logincount
                    , SUM(CASE WHEN AAA.sec11_12 != 0 THEN 1 ELSE 0 END ) as sec11_12_logincount
                    , SUM(CASE WHEN AAA.sec12_13 != 0 THEN 1 ELSE 0 END ) as sec12_13_logincount
                    , SUM(CASE WHEN AAA.sec13_14 != 0 THEN 1 ELSE 0 END ) as sec13_14_logincount
                    , SUM(CASE WHEN AAA.sec14_15 != 0 THEN 1 ELSE 0 END ) as sec14_15_logincount
                    , SUM(CASE WHEN AAA.sec15_16 != 0 THEN 1 ELSE 0 END ) as sec15_16_logincount
                    , SUM(CASE WHEN AAA.sec16_17 != 0 THEN 1 ELSE 0 END ) as sec16_17_logincount
                    , SUM(CASE WHEN AAA.sec17_18 != 0 THEN 1 ELSE 0 END ) as sec17_18_logincount
                    , SUM(CASE WHEN AAA.sec18_19 != 0 THEN 1 ELSE 0 END ) as sec18_19_logincount
                    , SUM(CASE WHEN AAA.sec19_20 != 0 THEN 1 ELSE 0 END ) as sec19_20_logincount
                    , SUM(CASE WHEN AAA.sec20_21 != 0 THEN 1 ELSE 0 END ) as sec20_21_logincount
                    , SUM(CASE WHEN AAA.sec21_22 != 0 THEN 1 ELSE 0 END ) as sec21_22_logincount
                    , SUM(CASE WHEN AAA.sec22_23 != 0 THEN 1 ELSE 0 END ) as sec22_23_logincount
                    , SUM(CASE WHEN AAA.sec23_24 != 0 THEN 1 ELSE 0 END ) as sec23_24_logincount
                    , SUM(CASE WHEN AAA.secday != 0 THEN 1 ELSE 0 END ) as secday_logincount
                FROM logincountbyaccountid AAA
                GROUP BY
                    AAA.dt
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour18_19'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec18_19_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour19_20'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec19_20_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour20_21'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec20_21_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour21_22'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec21_22_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour22_23'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec22_23_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour23_24'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , sec23_24_logincount::text as value
            FROM logincountbyeveryhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logincountbyeveryhour'::text as datatype1
                , country::text as datatype2
                , 'hour00_24'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , secday_logincount::text as value
            FROM logincountbyeveryhour ;

        
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make113DataSQL(self, makeInfo):
        insertSQL = """
            with basic_data as (
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), maxlogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), maxlogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , MAX(AA.hour00_01) as hour00_01
                    , MAX(AA.hour01_02) as hour01_02
                    , MAX(AA.hour02_03) as hour02_03
                    , MAX(AA.hour03_04) as hour03_04
                    , MAX(AA.hour04_05) as hour04_05
                    , MAX(AA.hour05_06) as hour05_06
                    , MAX(AA.hour06_07) as hour06_07
                    , MAX(AA.hour07_08) as hour07_08
                    , MAX(AA.hour08_09) as hour08_09
                    , MAX(AA.hour09_10) as hour09_10
                    , MAX(AA.hour10_11) as hour10_11
                    , MAX(AA.hour11_12) as hour11_12
                    , MAX(AA.hour12_13) as hour12_13
                    , MAX(AA.hour13_14) as hour13_14
                    , MAX(AA.hour14_15) as hour14_15
                    , MAX(AA.hour15_16) as hour15_16
                    , MAX(AA.hour16_17) as hour16_17
                    , MAX(AA.hour17_18) as hour17_18
                    , MAX(AA.hour18_19) as hour18_19
                    , MAX(AA.hour19_20) as hour19_20
                    , MAX(AA.hour20_21) as hour20_21
                    , MAX(AA.hour21_22) as hour21_22
                    , MAX(AA.hour22_23) as hour22_23
                    , MAX(AA.hour23_24) as hour23_24
                FROM maxlogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour00_01'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour00_01::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour01_02'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour01_02::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour02_03'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour02_03::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour03_04'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour03_04::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour04_05'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour04_05::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour05_06'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour05_06::text as value
            FROM maxlogincountbyhour ;
            
            
            with basic_data as (
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), maxlogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), maxlogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , MAX(AA.hour00_01) as hour00_01
                    , MAX(AA.hour01_02) as hour01_02
                    , MAX(AA.hour02_03) as hour02_03
                    , MAX(AA.hour03_04) as hour03_04
                    , MAX(AA.hour04_05) as hour04_05
                    , MAX(AA.hour05_06) as hour05_06
                    , MAX(AA.hour06_07) as hour06_07
                    , MAX(AA.hour07_08) as hour07_08
                    , MAX(AA.hour08_09) as hour08_09
                    , MAX(AA.hour09_10) as hour09_10
                    , MAX(AA.hour10_11) as hour10_11
                    , MAX(AA.hour11_12) as hour11_12
                    , MAX(AA.hour12_13) as hour12_13
                    , MAX(AA.hour13_14) as hour13_14
                    , MAX(AA.hour14_15) as hour14_15
                    , MAX(AA.hour15_16) as hour15_16
                    , MAX(AA.hour16_17) as hour16_17
                    , MAX(AA.hour17_18) as hour17_18
                    , MAX(AA.hour18_19) as hour18_19
                    , MAX(AA.hour19_20) as hour19_20
                    , MAX(AA.hour20_21) as hour20_21
                    , MAX(AA.hour21_22) as hour21_22
                    , MAX(AA.hour22_23) as hour22_23
                    , MAX(AA.hour23_24) as hour23_24
                FROM maxlogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour06_07'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour06_07::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour07_08'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour07_08::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour08_09'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour08_09::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour09_10'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour09_10::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour10_11'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour10_11::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour11_12'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour11_12::text as value
            FROM maxlogincountbyhour;
            
            
            with basic_data as (
                SELECT
                    AA.dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), maxlogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), maxlogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , MAX(AA.hour00_01) as hour00_01
                    , MAX(AA.hour01_02) as hour01_02
                    , MAX(AA.hour02_03) as hour02_03
                    , MAX(AA.hour03_04) as hour03_04
                    , MAX(AA.hour04_05) as hour04_05
                    , MAX(AA.hour05_06) as hour05_06
                    , MAX(AA.hour06_07) as hour06_07
                    , MAX(AA.hour07_08) as hour07_08
                    , MAX(AA.hour08_09) as hour08_09
                    , MAX(AA.hour09_10) as hour09_10
                    , MAX(AA.hour10_11) as hour10_11
                    , MAX(AA.hour11_12) as hour11_12
                    , MAX(AA.hour12_13) as hour12_13
                    , MAX(AA.hour13_14) as hour13_14
                    , MAX(AA.hour14_15) as hour14_15
                    , MAX(AA.hour15_16) as hour15_16
                    , MAX(AA.hour16_17) as hour16_17
                    , MAX(AA.hour17_18) as hour17_18
                    , MAX(AA.hour18_19) as hour18_19
                    , MAX(AA.hour19_20) as hour19_20
                    , MAX(AA.hour20_21) as hour20_21
                    , MAX(AA.hour21_22) as hour21_22
                    , MAX(AA.hour22_23) as hour22_23
                    , MAX(AA.hour23_24) as hour23_24
                FROM maxlogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour12_13'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour12_13::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour13_14'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour13_14::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour14_15'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour14_15::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour15_16'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour15_16::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour16_17'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour16_17::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour17_18'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour17_18::text as value
            FROM maxlogincountbyhour;
            
            
            with basic_data as (
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), maxlogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), maxlogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , MAX(AA.hour00_01) as hour00_01
                    , MAX(AA.hour01_02) as hour01_02
                    , MAX(AA.hour02_03) as hour02_03
                    , MAX(AA.hour03_04) as hour03_04
                    , MAX(AA.hour04_05) as hour04_05
                    , MAX(AA.hour05_06) as hour05_06
                    , MAX(AA.hour06_07) as hour06_07
                    , MAX(AA.hour07_08) as hour07_08
                    , MAX(AA.hour08_09) as hour08_09
                    , MAX(AA.hour09_10) as hour09_10
                    , MAX(AA.hour10_11) as hour10_11
                    , MAX(AA.hour11_12) as hour11_12
                    , MAX(AA.hour12_13) as hour12_13
                    , MAX(AA.hour13_14) as hour13_14
                    , MAX(AA.hour14_15) as hour14_15
                    , MAX(AA.hour15_16) as hour15_16
                    , MAX(AA.hour16_17) as hour16_17
                    , MAX(AA.hour17_18) as hour17_18
                    , MAX(AA.hour18_19) as hour18_19
                    , MAX(AA.hour19_20) as hour19_20
                    , MAX(AA.hour20_21) as hour20_21
                    , MAX(AA.hour21_22) as hour21_22
                    , MAX(AA.hour22_23) as hour22_23
                    , MAX(AA.hour23_24) as hour23_24
                FROM maxlogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour18_19'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour18_19::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour19_20'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour19_20::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour20_21'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour20_21::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour21_22'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour21_22::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour22_23'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour22_23::text as value
            FROM maxlogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'maxlogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour23_24'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour23_24::text as value
            FROM maxlogincountbyhour ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make114DataSQL(self, makeInfo):
        insertSQL = """
            with basic_data as (
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1136 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1137 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), avglogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), avglogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , ROUND(AVG(AA.hour00_01),0) as hour00_01
                    , ROUND(AVG(AA.hour01_02),0) as hour01_02
                    , ROUND(AVG(AA.hour02_03),0) as hour02_03
                    , ROUND(AVG(AA.hour03_04),0) as hour03_04
                    , ROUND(AVG(AA.hour04_05),0) as hour04_05
                    , ROUND(AVG(AA.hour05_06),0) as hour05_06
                    , ROUND(AVG(AA.hour06_07),0) as hour06_07
                    , ROUND(AVG(AA.hour07_08),0) as hour07_08
                    , ROUND(AVG(AA.hour08_09),0) as hour08_09
                    , ROUND(AVG(AA.hour09_10),0) as hour09_10
                    , ROUND(AVG(AA.hour10_11),0) as hour10_11
                    , ROUND(AVG(AA.hour11_12),0) as hour11_12
                    , ROUND(AVG(AA.hour12_13),0) as hour12_13
                    , ROUND(AVG(AA.hour13_14),0) as hour13_14
                    , ROUND(AVG(AA.hour14_15),0) as hour14_15
                    , ROUND(AVG(AA.hour15_16),0) as hour15_16
                    , ROUND(AVG(AA.hour16_17),0) as hour16_17
                    , ROUND(AVG(AA.hour17_18),0) as hour17_18
                    , ROUND(AVG(AA.hour18_19),0) as hour18_19
                    , ROUND(AVG(AA.hour19_20),0) as hour19_20
                    , ROUND(AVG(AA.hour20_21),0) as hour20_21
                    , ROUND(AVG(AA.hour21_22),0) as hour21_22
                    , ROUND(AVG(AA.hour22_23),0) as hour22_23
                    , ROUND(AVG(AA.hour23_24),0) as hour23_24
                FROM avglogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour00_01'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour00_01::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour01_02'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour01_02::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour02_03'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour02_03::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour03_04'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour03_04::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour04_05'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour04_05::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour05_06'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour05_06::text as value
            FROM avglogincountbyhour ;
            
            
            with basic_data as (
                SELECT
                    AA.dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), avglogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), avglogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , ROUND(AVG(AA.hour00_01),0) as hour00_01
                    , ROUND(AVG(AA.hour01_02),0) as hour01_02
                    , ROUND(AVG(AA.hour02_03),0) as hour02_03
                    , ROUND(AVG(AA.hour03_04),0) as hour03_04
                    , ROUND(AVG(AA.hour04_05),0) as hour04_05
                    , ROUND(AVG(AA.hour05_06),0) as hour05_06
                    , ROUND(AVG(AA.hour06_07),0) as hour06_07
                    , ROUND(AVG(AA.hour07_08),0) as hour07_08
                    , ROUND(AVG(AA.hour08_09),0) as hour08_09
                    , ROUND(AVG(AA.hour09_10),0) as hour09_10
                    , ROUND(AVG(AA.hour10_11),0) as hour10_11
                    , ROUND(AVG(AA.hour11_12),0) as hour11_12
                    , ROUND(AVG(AA.hour12_13),0) as hour12_13
                    , ROUND(AVG(AA.hour13_14),0) as hour13_14
                    , ROUND(AVG(AA.hour14_15),0) as hour14_15
                    , ROUND(AVG(AA.hour15_16),0) as hour15_16
                    , ROUND(AVG(AA.hour16_17),0) as hour16_17
                    , ROUND(AVG(AA.hour17_18),0) as hour17_18
                    , ROUND(AVG(AA.hour18_19),0) as hour18_19
                    , ROUND(AVG(AA.hour19_20),0) as hour19_20
                    , ROUND(AVG(AA.hour20_21),0) as hour20_21
                    , ROUND(AVG(AA.hour21_22),0) as hour21_22
                    , ROUND(AVG(AA.hour22_23),0) as hour22_23
                    , ROUND(AVG(AA.hour23_24),0) as hour23_24
                FROM avglogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour06_07'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour06_07::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour07_08'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour07_08::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour08_09'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour08_09::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour09_10'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour09_10::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour10_11'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour10_11::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour11_12'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour11_12::text as value
            FROM avglogincountbyhour;
            
            
            with basic_data as (
                SELECT
                    AA.dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), avglogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), avglogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , ROUND(AVG(AA.hour00_01),0) as hour00_01
                    , ROUND(AVG(AA.hour01_02),0) as hour01_02
                    , ROUND(AVG(AA.hour02_03),0) as hour02_03
                    , ROUND(AVG(AA.hour03_04),0) as hour03_04
                    , ROUND(AVG(AA.hour04_05),0) as hour04_05
                    , ROUND(AVG(AA.hour05_06),0) as hour05_06
                    , ROUND(AVG(AA.hour06_07),0) as hour06_07
                    , ROUND(AVG(AA.hour07_08),0) as hour07_08
                    , ROUND(AVG(AA.hour08_09),0) as hour08_09
                    , ROUND(AVG(AA.hour09_10),0) as hour09_10
                    , ROUND(AVG(AA.hour10_11),0) as hour10_11
                    , ROUND(AVG(AA.hour11_12),0) as hour11_12
                    , ROUND(AVG(AA.hour12_13),0) as hour12_13
                    , ROUND(AVG(AA.hour13_14),0) as hour13_14
                    , ROUND(AVG(AA.hour14_15),0) as hour14_15
                    , ROUND(AVG(AA.hour15_16),0) as hour15_16
                    , ROUND(AVG(AA.hour16_17),0) as hour16_17
                    , ROUND(AVG(AA.hour17_18),0) as hour17_18
                    , ROUND(AVG(AA.hour18_19),0) as hour18_19
                    , ROUND(AVG(AA.hour19_20),0) as hour19_20
                    , ROUND(AVG(AA.hour20_21),0) as hour20_21
                    , ROUND(AVG(AA.hour21_22),0) as hour21_22
                    , ROUND(AVG(AA.hour22_23),0) as hour22_23
                    , ROUND(AVG(AA.hour23_24),0) as hour23_24
                FROM avglogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour12_13'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour12_13::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour13_14'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour13_14::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour14_15'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour14_15::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour15_16'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour15_16::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour16_17'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour16_17::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour17_18'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour17_18::text as value
            FROM avglogincountbyhour;
            
            
            with basic_data as (
                SELECT
                    AA.dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , AA.UniqueInt_1 as hour00_01
                    , AA.UniqueInt_2 as hour01_02
                    , AA.UniqueInt_3 as hour02_03
                    , AA.UniqueInt_4 as hour03_04
                    , AA.UniqueInt_5 as hour04_05
                    , AA.UniqueInt_6 as hour05_06
                    , AA.UniqueInt_7 as hour06_07
                    , AA.UniqueInt_8 as hour07_08
                    , AA.UniqueInt_9 as hour08_09
                    , AA.UniqueInt_10 as hour09_10
                    , AA.UniqueInt_11 as hour10_11
                    , AA.UniqueInt_12 as hour11_12
                    , 0 as  hour12_13 --
                    , 0 as  hour13_14 --
                    , 0 as  hour14_15 --
                    , 0 as  hour15_16 --
                    , 0 as  hour16_17 --
                    , 0 as  hour17_18 --
                    , 0 as  hour18_19 --
                    , 0 as  hour19_20 --
                    , 0 as  hour20_21 --
                    , 0 as  hour21_22 --
                    , 0 as  hour22_23 --
                    , 0 as  hour23_24 --
                FROM [:GameName].bu1134 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
                UNION ALL
                SELECT
                    AA.dt as dt
                    , AA.world as world
                    , lower(AA.commondata_7) as country
                    , 0 as  hour00_01 --
                    , 0 as  hour01_02 --
                    , 0 as  hour02_03 --
                    , 0 as  hour03_04 --
                    , 0 as  hour04_05 --
                    , 0 as  hour05_06 --
                    , 0 as  hour06_07 --
                    , 0 as  hour07_08 --
                    , 0 as  hour08_09 --
                    , 0 as  hour09_10 --
                    , 0 as  hour10_11 --
                    , 0 as  hour11_12 --
                    , UniqueInt_1 as hour12_13 --
                    , UniqueInt_2 as hour13_14 --
                    , UniqueInt_3 as hour14_15 --
                    , UniqueInt_4 as hour15_16 --
                    , UniqueInt_5 as hour16_17 --
                    , UniqueInt_6 as hour17_18 --
                    , UniqueInt_7 as hour18_19 --
                    , UniqueInt_8 as hour19_20 --
                    , UniqueInt_9 as hour20_21 --
                    , UniqueInt_10 as hour21_22 --
                    , UniqueInt_11 as hour22_23 --
                    , UniqueInt_12 as hour23_24 --
                FROM [:GameName].bu1135 AA
                WHERE 1 = 1
                    and aa.dt >= '[:StartDateLine]'
                    and aa.dt <= '[:EndDateLine]'
            ), avglogincountbyhour_day as (
                SELECT
                    AA.dt
                    , AA.country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
                    , AA.country
                UNION ALL
                SELECT
                    AA.dt
                    , 'all' as country
                    , SUM(AA.hour00_01) as hour00_01
                    , SUM(AA.hour01_02) as hour01_02
                    , SUM(AA.hour02_03) as hour02_03
                    , SUM(AA.hour03_04) as hour03_04
                    , SUM(AA.hour04_05) as hour04_05
                    , SUM(AA.hour05_06) as hour05_06
                    , SUM(AA.hour06_07) as hour06_07
                    , SUM(AA.hour07_08) as hour07_08
                    , SUM(AA.hour08_09) as hour08_09
                    , SUM(AA.hour09_10) as hour09_10
                    , SUM(AA.hour10_11) as hour10_11
                    , SUM(AA.hour11_12) as hour11_12
                    , SUM(AA.hour12_13) as hour12_13
                    , SUM(AA.hour13_14) as hour13_14
                    , SUM(AA.hour14_15) as hour14_15
                    , SUM(AA.hour15_16) as hour15_16
                    , SUM(AA.hour16_17) as hour16_17
                    , SUM(AA.hour17_18) as hour17_18
                    , SUM(AA.hour18_19) as hour18_19
                    , SUM(AA.hour19_20) as hour19_20
                    , SUM(AA.hour20_21) as hour20_21
                    , SUM(AA.hour21_22) as hour21_22
                    , SUM(AA.hour22_23) as hour22_23
                    , SUM(AA.hour23_24) as hour23_24
                FROM basic_data AA
                GROUP BY
                    AA.dt
            ), avglogincountbyhour as (
                SELECT
                    '[:DataReportName]' as dt
                    , AA.country
                    , ROUND(AVG(AA.hour00_01),0) as hour00_01
                    , ROUND(AVG(AA.hour01_02),0) as hour01_02
                    , ROUND(AVG(AA.hour02_03),0) as hour02_03
                    , ROUND(AVG(AA.hour03_04),0) as hour03_04
                    , ROUND(AVG(AA.hour04_05),0) as hour04_05
                    , ROUND(AVG(AA.hour05_06),0) as hour05_06
                    , ROUND(AVG(AA.hour06_07),0) as hour06_07
                    , ROUND(AVG(AA.hour07_08),0) as hour07_08
                    , ROUND(AVG(AA.hour08_09),0) as hour08_09
                    , ROUND(AVG(AA.hour09_10),0) as hour09_10
                    , ROUND(AVG(AA.hour10_11),0) as hour10_11
                    , ROUND(AVG(AA.hour11_12),0) as hour11_12
                    , ROUND(AVG(AA.hour12_13),0) as hour12_13
                    , ROUND(AVG(AA.hour13_14),0) as hour13_14
                    , ROUND(AVG(AA.hour14_15),0) as hour14_15
                    , ROUND(AVG(AA.hour15_16),0) as hour15_16
                    , ROUND(AVG(AA.hour16_17),0) as hour16_17
                    , ROUND(AVG(AA.hour17_18),0) as hour17_18
                    , ROUND(AVG(AA.hour18_19),0) as hour18_19
                    , ROUND(AVG(AA.hour19_20),0) as hour19_20
                    , ROUND(AVG(AA.hour20_21),0) as hour20_21
                    , ROUND(AVG(AA.hour21_22),0) as hour21_22
                    , ROUND(AVG(AA.hour22_23),0) as hour22_23
                    , ROUND(AVG(AA.hour23_24),0) as hour23_24
                FROM avglogincountbyhour_day AA
                GROUP BY
                    AA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour18_19'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour18_19::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour19_20'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour19_20::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour20_21'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour20_21::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour21_22'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour21_22::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour22_23'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour22_23::text as value
            FROM avglogincountbyhour
            union all
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]'::text as gamename
                , '[:ReportCode]'::text as reportcode
                , 'avglogincountbyhour'::text as datatype1
                , country::text as datatype2
                , 'hour23_24'::text as datatype3
                , null::text as datatype4
                , null::text as datatype5
                , hour23_24::text as value
            FROM avglogincountbyhour ;
        """
        return "OrderInsert", [insertSQL]