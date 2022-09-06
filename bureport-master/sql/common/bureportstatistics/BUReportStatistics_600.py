
class BUReportStatistics_600() :

    @classmethod
    def make601DataSQL(self, makeInfo):
        insertSQL = """
            with basic_data as (
                SELECT
                    AAA.dt
                    , AAA.serviceaccountid
                    , AAA.country as country
                    , sum(AAA.shoppoint) as shoppoint
                FROM (
                    SELECT
                        '[:DataReportName]' as dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , sum(AA.uniqueint_1) as  shoppoint
                    FROM [:GameName].bu6002 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.commondata_1
                        , lower(AA.commondata_7)
                    UNION ALL
                    select
                        '[:DataReportName]' as dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , 0 as shoppoint
                    from [:GameName].bu1003 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.commondata_1
                        , lower(AA.commondata_7)
                ) AAA
                group by
                    AAA.dt
                    , AAA.serviceaccountid
                    , AAA.country
            ) , revenue_data as (
                select
                    AAAA.dt
                    , 'all' as country
                    , sum(AAAA.shoppoint) as totalrevenue
                    , SUM(1) as loginaccountcount
                    , round(sum(AAAA.shoppoint) / SUM(1),2) as loginaccountarpu
                    , SUM(case when AAAA.grouptype != 'E' then 1 else 0 end) as paidaccountcount
                    , round(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end)/SUM(1.0),4) as paidaccountrate
                    , round(sum(AAAA.shoppoint) / greatest(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end),1),2) as paidaccountarpu
                    , SUM(case when AAAA.grouptype = 'A' then 1 else 0 end) as paid1001accountcount
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'A' then 1 else 0 end),1),2)::double precision as paid1001accountarpu
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1001revenuerate
                    , SUM(case when AAAA.grouptype = 'B' then 1 else 0 end) as paid501accountcount
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'B' then 1 else 0 end),1),2)::double precision as paid501accountarpu
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid501revenuerate
                    , SUM(case when AAAA.grouptype = 'C' then 1 else 0 end) as paid101accountcount
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'C' then 1 else 0 end),1),2)::double precision as paid101accountarpu
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid101revenuerate
                    , SUM(case when AAAA.grouptype = 'D' then 1 else 0 end) as paid1accountcount
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'D' then 1 else 0 end),1),2)::double precision as paid1accountarpu
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1revenuerate
                from (
                    select
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                        , sum(AAA.shoppoint) as shoppoint
                        , case
                            when sum(AAA.shoppoint) >= 1001 then 'A'
                            when sum(AAA.shoppoint) >= 501 then 'B'
                            when sum(AAA.shoppoint) >= 101 then 'C'
                            when sum(AAA.shoppoint) >= 1 then 'D'
                            else 'E'
                        end as grouptype
                    from basic_data AAA
                    where 1 = 1
                    group by
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                ) AAAA
                group by
                    AAAA.dt
                union all
                select
                    AAAA.dt
                    , AAAA.country
                    , sum(AAAA.shoppoint) as totalrevenue
                    , SUM(1) as loginaccountcount
                    , round(sum(AAAA.shoppoint) / SUM(1),2) as loginaccountarpu
                    , SUM(case when AAAA.grouptype != 'E' then 1 else 0 end) as paidaccountcount
                    , round(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end)/SUM(1.0),4) as paidaccountrate
                    , round(sum(AAAA.shoppoint) / greatest(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end),1),2) as paidaccountarpu
                    , SUM(case when AAAA.grouptype = 'A' then 1 else 0 end) as paid1001accountcount
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'A' then 1 else 0 end),1),2)::double precision as paid1001accountarpu
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1001revenuerate
                    , SUM(case when AAAA.grouptype = 'B' then 1 else 0 end) as paid501accountcount
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'B' then 1 else 0 end),1),2)::double precision as paid501accountarpu
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid501revenuerate
                    , SUM(case when AAAA.grouptype = 'C' then 1 else 0 end) as paid101accountcount
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'C' then 1 else 0 end),1),2)::double precision as paid101accountarpu
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid101revenuerate
                    , SUM(case when AAAA.grouptype = 'D' then 1 else 0 end) as paid1accountcount
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'D' then 1 else 0 end),1),2)::double precision as paid1accountarpu
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1revenuerate
                from (
                    select
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                        , sum(AAA.shoppoint) as shoppoint
                        , case
                            when sum(AAA.shoppoint) >= 1001 then 'A'
                            when sum(AAA.shoppoint) >= 501 then 'B'
                            when sum(AAA.shoppoint) >= 101 then 'C'
                            when sum(AAA.shoppoint) >= 1 then 'D'
                            else 'E'
                        end as grouptype
                    from basic_data AAA
                    where 1 = 1
                    group by
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                ) AAAA
                WHERE 1 = 1
                group by
                    AAAA.dt
                    , AAAA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'totalrevenue' as datatype3
                , null as datatype4
                , null as datatype5
                , totalrevenue as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'loginaccountcount' as datatype3
                , null as datatype4
                , null as datatype5
                , loginaccountcount as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'loginaccountarpu' as datatype3
                , null as datatype4
                , null as datatype5
                , loginaccountarpu as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paidaccountcount' as datatype3
                , null as datatype4
                , null as datatype5
                , paidaccountcount as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paidaccountrate' as datatype3
                , null as datatype4
                , null as datatype5
                , paidaccountrate as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paidaccountarpu' as datatype3
                , null as datatype4
                , null as datatype5
                , paidaccountarpu as value
            FROM revenue_data AAA ;
            
            
            with basic_data as (
                SELECT
                    AAA.dt
                    , AAA.serviceaccountid
                    , AAA.country as country
                    , sum(AAA.shoppoint) as shoppoint
                FROM (
                    SELECT
                        '[:DataReportName]' as dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , sum(AA.uniqueint_1) as  shoppoint
                    FROM [:GameName].bu6002 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.commondata_1
                        , AA.commondata_7
                    UNION ALL
                    select
                        '[:DataReportName]' as dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , 0 as shoppoint
                    from [:GameName].bu1003 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.commondata_1
                        , AA.commondata_7
                ) AAA
                group by
                    AAA.dt
                    , AAA.serviceaccountid
                    , AAA.country
            ) , revenue_data as (
                select
                    AAAA.dt
                    , 'all' as country
                    , sum(AAAA.shoppoint) as totalrevenue
                    , SUM(1) as loginaccountcount
                    , round(sum(AAAA.shoppoint) / SUM(1),2) as loginaccountarpu
                    , SUM(case when AAAA.grouptype != 'E' then 1 else 0 end) as paidaccountcount
                    , round(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end)/SUM(1.0),4) as paidaccountrate
                    , round(sum(AAAA.shoppoint) / greatest(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end),1),2) as paidaccountarpu
                    , SUM(case when AAAA.grouptype = 'A' then 1 else 0 end) as paid1001accountcount
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'A' then 1 else 0 end),1),2)::double precision as paid1001accountarpu
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1001revenuerate
                    , SUM(case when AAAA.grouptype = 'B' then 1 else 0 end) as paid501accountcount
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'B' then 1 else 0 end),1),2)::double precision as paid501accountarpu
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid501revenuerate
                    , SUM(case when AAAA.grouptype = 'C' then 1 else 0 end) as paid101accountcount
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'C' then 1 else 0 end),1),2)::double precision as paid101accountarpu
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid101revenuerate
                    , SUM(case when AAAA.grouptype = 'D' then 1 else 0 end) as paid1accountcount
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'D' then 1 else 0 end),1),2)::double precision as paid1accountarpu
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1revenuerate
                from (
                    select
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                        , sum(AAA.shoppoint) as shoppoint
                        , case
                            when sum(AAA.shoppoint) >= 1001 then 'A'
                            when sum(AAA.shoppoint) >= 501 then 'B'
                            when sum(AAA.shoppoint) >= 101 then 'C'
                            when sum(AAA.shoppoint) >= 1 then 'D'
                            else 'E'
                        end as grouptype
                    from basic_data AAA
                    where 1 = 1
                    group by
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                ) AAAA
                group by
                    AAAA.dt
                union all
                select
                    AAAA.dt
                    , AAAA.country
                    , sum(AAAA.shoppoint) as totalrevenue
                    , SUM(1) as loginaccountcount
                    , round(sum(AAAA.shoppoint) / SUM(1),2) as loginaccountarpu
                    , SUM(case when AAAA.grouptype != 'E' then 1 else 0 end) as paidaccountcount
                    , round(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end)/SUM(1.0),4) as paidaccountrate
                    , round(sum(AAAA.shoppoint) / greatest(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end),1),2) as paidaccountarpu
                    , SUM(case when AAAA.grouptype = 'A' then 1 else 0 end) as paid1001accountcount
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'A' then 1 else 0 end),1),2)::double precision as paid1001accountarpu
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1001revenuerate
                    , SUM(case when AAAA.grouptype = 'B' then 1 else 0 end) as paid501accountcount
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'B' then 1 else 0 end),1),2)::double precision as paid501accountarpu
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid501revenuerate
                    , SUM(case when AAAA.grouptype = 'C' then 1 else 0 end) as paid101accountcount
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'C' then 1 else 0 end),1),2)::double precision as paid101accountarpu
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid101revenuerate
                    , SUM(case when AAAA.grouptype = 'D' then 1 else 0 end) as paid1accountcount
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'D' then 1 else 0 end),1),2)::double precision as paid1accountarpu
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1revenuerate
                from (
                    select
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                        , sum(AAA.shoppoint) as shoppoint
                        , case
                            when sum(AAA.shoppoint) >= 1001 then 'A'
                            when sum(AAA.shoppoint) >= 501 then 'B'
                            when sum(AAA.shoppoint) >= 101 then 'C'
                            when sum(AAA.shoppoint) >= 1 then 'D'
                            else 'E'
                        end as grouptype
                    from basic_data AAA
                    where 1 = 1
                    group by
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                ) AAAA
                WHERE 1 = 1
                group by
                    AAAA.dt
                    , AAAA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid1001accountcount' as datatype3
                , null as datatype4
                , null as datatype5
                , paid1001accountcount as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid1001accountarpu' as datatype3
                , null as datatype4
                , null as datatype5
                , paid1001accountarpu as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid1001revenuerate' as datatype3
                , null as datatype4
                , null as datatype5
                , paid1001revenuerate as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid501accountcount' as datatype3
                , null as datatype4
                , null as datatype5
                , paid501accountcount as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid501accountarpu' as datatype3
                , null as datatype4
                , null as datatype5
                , paid501accountarpu as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid501revenuerate' as datatype3
                , null as datatype4
                , null as datatype5
                , paid501revenuerate as value
            FROM revenue_data AAA ;
            
            
            with basic_data as (
                SELECT
                    AAA.dt
                    , AAA.serviceaccountid
                    , AAA.country as country
                    , sum(AAA.shoppoint) as shoppoint
                FROM (
                    SELECT
                        '[:DataReportName]' as dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , sum(AA.uniqueint_1) as  shoppoint
                    FROM [:GameName].bu6002 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.commondata_1
                        , AA.commondata_7
                    UNION ALL
                    select
                        '[:DataReportName]' as dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , 0 as shoppoint
                    from [:GameName].bu1003 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.commondata_1
                        , AA.commondata_7
                ) AAA
                group by
                    AAA.dt
                    , AAA.serviceaccountid
                    , AAA.country
            ) , revenue_data as (
                select
                    AAAA.dt
                    , 'all' as country
                    , sum(AAAA.shoppoint) as totalrevenue
                    , SUM(1) as loginaccountcount
                    , round(sum(AAAA.shoppoint) / SUM(1),2) as loginaccountarpu
                    , SUM(case when AAAA.grouptype != 'E' then 1 else 0 end) as paidaccountcount
                    , round(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end)/SUM(1.0),4) as paidaccountrate
                    , round(sum(AAAA.shoppoint) / greatest(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end),1),2) as paidaccountarpu
                    , SUM(case when AAAA.grouptype = 'A' then 1 else 0 end) as paid1001accountcount
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'A' then 1 else 0 end),1),2)::double precision as paid1001accountarpu
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1001revenuerate
                    , SUM(case when AAAA.grouptype = 'B' then 1 else 0 end) as paid501accountcount
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'B' then 1 else 0 end),1),2)::double precision as paid501accountarpu
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid501revenuerate
                    , SUM(case when AAAA.grouptype = 'C' then 1 else 0 end) as paid101accountcount
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'C' then 1 else 0 end),1),2)::double precision as paid101accountarpu
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid101revenuerate
                    , SUM(case when AAAA.grouptype = 'D' then 1 else 0 end) as paid1accountcount
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'D' then 1 else 0 end),1),2)::double precision as paid1accountarpu
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1revenuerate
                from (
                    select
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                        , sum(AAA.shoppoint) as shoppoint
                        , case
                            when sum(AAA.shoppoint) >= 1001 then 'A'
                            when sum(AAA.shoppoint) >= 501 then 'B'
                            when sum(AAA.shoppoint) >= 101 then 'C'
                            when sum(AAA.shoppoint) >= 1 then 'D'
                            else 'E'
                        end as grouptype
                    from basic_data AAA
                    where 1 = 1
                    group by
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                ) AAAA
                group by
                    AAAA.dt
                union all
                select
                    AAAA.dt
                    , AAAA.country
                    , sum(AAAA.shoppoint) as totalrevenue
                    , SUM(1) as loginaccountcount
                    , round(sum(AAAA.shoppoint) / SUM(1),2) as loginaccountarpu
                    , SUM(case when AAAA.grouptype != 'E' then 1 else 0 end) as paidaccountcount
                    , round(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end)/SUM(1.0),4) as paidaccountrate
                    , round(sum(AAAA.shoppoint) / greatest(SUM(case when AAAA.grouptype != 'E' then 1 else 0 end),1),2) as paidaccountarpu
                    , SUM(case when AAAA.grouptype = 'A' then 1 else 0 end) as paid1001accountcount
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'A' then 1 else 0 end),1),2)::double precision as paid1001accountarpu
                    , round(SUM(case when AAAA.grouptype = 'A' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1001revenuerate
                    , SUM(case when AAAA.grouptype = 'B' then 1 else 0 end) as paid501accountcount
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'B' then 1 else 0 end),1),2)::double precision as paid501accountarpu
                    , round(SUM(case when AAAA.grouptype = 'B' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid501revenuerate
                    , SUM(case when AAAA.grouptype = 'C' then 1 else 0 end) as paid101accountcount
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'C' then 1 else 0 end),1),2)::double precision as paid101accountarpu
                    , round(SUM(case when AAAA.grouptype = 'C' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid101revenuerate
                    , SUM(case when AAAA.grouptype = 'D' then 1 else 0 end) as paid1accountcount
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(SUM(case when AAAA.grouptype = 'D' then 1 else 0 end),1),2)::double precision as paid1accountarpu
                    , round(SUM(case when AAAA.grouptype = 'D' then shoppoint else 0 end) / greatest(sum(AAAA.shoppoint),1),4) as paid1revenuerate
                from (
                    select
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                        , sum(AAA.shoppoint) as shoppoint
                        , case
                            when sum(AAA.shoppoint) >= 1001 then 'A'
                            when sum(AAA.shoppoint) >= 501 then 'B'
                            when sum(AAA.shoppoint) >= 101 then 'C'
                            when sum(AAA.shoppoint) >= 1 then 'D'
                            else 'E'
                        end as grouptype
                    from basic_data AAA
                    where 1 = 1
                    group by
                        AAA.dt
                        , AAA.serviceaccountid
                        , AAA.country
                ) AAAA
                WHERE 1 = 1
                group by
                    AAAA.dt
                    , AAAA.country
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid101accountcount' as datatype3
                , null as datatype4
                , null as datatype5
                , paid101accountcount as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid101accountarpu' as datatype3
                , null as datatype4
                , null as datatype5
                , paid101accountarpu as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid101revenuerate' as datatype3
                , null as datatype4
                , null as datatype5
                , paid101revenuerate as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid1accountcount' as datatype3
                , null as datatype4
                , null as datatype5
                , paid1accountcount as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid1accountarpu' as datatype3
                , null as datatype4
                , null as datatype5
                , paid1accountarpu as value
            FROM revenue_data AAA
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'revenue' as datatype1
                , country as datatype2
                , 'paid1revenuerate' as datatype3
                , null as datatype4
                , null as datatype5
                , paid1revenuerate as value
            FROM revenue_data AAA ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make602DataSQL(self, makeInfo):
        insertSQL = """
            WITH basic_data AS (
                SELECT
                    AAA.serviceid
                    , MAX(AAA.country) AS country
                    , MAX(CASE WHEN AAA.dataname = '1003' AND AAA.dt >= '[:StartDateLine]' AND AAA.dt <= '[:EndDateLine]' THEN 1 ELSE 0 END ) AS NOWLOGIN
                    , MAX(CASE WHEN AAA.dataname = '1003' AND AAA.dt >= '[:PreStartDateLine]' AND AAA.dt <= '[:PreEndDateLine]' THEN 1 ELSE 0 END ) AS PRELOGIN
                    , MAX(CASE WHEN AAA.dataname = '1002' AND AAA.createtime >= '[:StartDateLine]' AND AAA.createtime <= '[:EndDateLine]' THEN 1 ELSE 0 END ) AS NEWLOGIN
                    , sum(CASE WHEN AAA.dataname = '6002' AND AAA.dt >= '[:StartDateLine]' AND AAA.dt <= '[:EndDateLine]' THEN AAA.paypoint ELSE 0 END ) AS paypoint
                FROM (
                    SELECT
                        DISTINCT '1003' AS dataname
                        , AA.dt
                        , AA.commondata_1 AS serviceid
                        , lower(AA.commondata_7) as country
                        , NULL AS createtime
                        , 0 as paypoint
                    FROM [:GameName].bu1003 AA
                    WHERE 1 = 1
                        AND ( 1 != 1
                            OR (AA.dt >= '[:StartDateLine]' AND AA.dt <= '[:EndDateLine]')
                            OR (AA.dt >= '[:PreStartDateLine]' AND AA.dt <= '[:PreEndDateLine]')
                        )
                    UNION ALL
            
                    SELECT
                        DISTINCT '1002' AS dataname
                        , AA.dt
                        , AA.commondata_1 AS serviceid
                        , lower(AA.commondata_7) as country
                        , to_char(AA.uniquetime_1,'YYYY-MM-DD') AS createtime
                        , 0 as paypoint
                    FROM [:GameName].bu1002 AA
                    WHERE 1 = 1
                        AND ( 1 != 1
                            OR (AA.dt >= '[:StartDateLine]' AND AA.dt <= '[:EndDateLine]')
                            OR (AA.dt >= '[:PreStartDateLine]' AND AA.dt <= '[:PreEndDateLine]')
                        )
                    UNION ALL
                    SELECT
                        '6002' AS dataname
                        , AA.dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , NULL AS createtime
                        , sum(AA.uniqueint_1) as  paypoint
                    FROM [:GameName].bu6002 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.dt
                        , AA.commondata_1
                        , lower(AA.commondata_7)
                ) AAA
                WHERE AAA.country = 'tw'
                GROUP BY
                    AAA.serviceid
            ), logintype_revenue_data as (
                SELECT
                    '[:DataReportName]' as dt
                    , 'all' AS logintype
                    , AAAA.country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                GROUP BY
                    AAAA.country
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'step' AS logintype
                    , AAAA.country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1 
                    AND ( 1 != 1 
                        OR (AAAA.nowlogin = 1 AND AAAA.prelogin = 1 )
                        OR (AAAA.nowlogin = 0 AND AAAA.paypoint != 0)
                    )
                GROUP BY
                    AAAA.country
                UNION ALL 
                SELECT
                    '[:DataReportName]' as dt
                    , 'reback' AS logintype
                    , AAAA.country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1 
                    AND AAAA.nowlogin = 1 
                    AND AAAA.prelogin = 0 
                    AND AAAA.newlogin = 0
                GROUP BY
                    AAAA.country
                UNION ALL 
                SELECT
                    '[:DataReportName]' as dt
                    , 'newcount' AS logintype
                    , AAAA.country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1 
                    AND AAAA.nowlogin = 1 
                    AND AAAA.prelogin = 0 
                    AND AAAA.newlogin = 1
                GROUP BY
                    AAAA.country
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'all' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'step' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                    AND ( 1 != 1
                        OR (AAAA.nowlogin = 1 AND AAAA.prelogin = 1 )
                        OR (AAAA.nowlogin = 0 AND AAAA.paypoint != 0)
                    )
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'reback' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                    AND AAAA.nowlogin = 1
                    AND AAAA.prelogin = 0
                    AND AAAA.newlogin = 0
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'newcount' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                    AND AAAA.nowlogin = 1
                    AND AAAA.prelogin = 0
                    AND AAAA.newlogin = 1
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'logincount' as datatype4
                , null as datatype5
                , AAAAA.logincount as value
            FROM logintype_revenue_data AAAAA	
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paycount' as datatype4
                , null as datatype5
                , AAAAA.paycount as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paypoint' as datatype4
                , null as datatype5
                , AAAAA.paypoint as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paidaccountrate' as datatype4
                , null as datatype5
                , AAAAA.paidaccountrate as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'loginaccountarpu' as datatype4
                , null as datatype5
                , AAAAA.loginaccountarpu as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paidaccountarpu' as datatype4
                , null as datatype5
                , AAAAA.paidaccountarpu as value
            FROM logintype_revenue_data AAAAA ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make602DataSQL_Mobile(self, makeInfo):
        insertSQL = """
            WITH basic_data AS (
                SELECT
                    AAA.serviceid
                    , MAX(AAA.country) AS country
                    , MAX(CASE WHEN AAA.dataname = '1003' AND AAA.dt >= '[:StartDateLine]' AND AAA.dt <= '[:EndDateLine]' THEN 1 ELSE 0 END ) AS NOWLOGIN
                    , MAX(CASE WHEN AAA.dataname = '1003' AND AAA.dt >= '[:PreStartDateLine]' AND AAA.dt <= '[:PreEndDateLine]' THEN 1 ELSE 0 END ) AS PRELOGIN
                    , MAX(CASE WHEN AAA.dataname = '1002' AND AAA.createtime >= '[:StartDateLine]' AND AAA.createtime <= '[:EndDateLine]' THEN 1 ELSE 0 END ) AS NEWLOGIN
                    , sum(CASE WHEN AAA.dataname = '6002' AND AAA.dt >= '[:StartDateLine]' AND AAA.dt <= '[:EndDateLine]' THEN AAA.paypoint ELSE 0 END ) AS paypoint
                FROM (
                    SELECT
                        DISTINCT '1003' AS dataname
                        , AA.dt
                        , AA.commondata_1 AS serviceid
                        , lower(AA.commondata_7) as country
                        , NULL AS createtime
                        , 0 as paypoint
                    FROM [:GameName].bu1003 AA
                    WHERE 1 = 1
                        AND ( 1 != 1
                            OR (AA.dt >= '[:StartDateLine]' AND AA.dt <= '[:EndDateLine]')
                            OR (AA.dt >= '[:PreStartDateLine]' AND AA.dt <= '[:PreEndDateLine]')
                        )
                    UNION ALL
                    SELECT
                        DISTINCT '1002' AS dataname
                        , AA.dt
                        , AA.commondata_1 AS serviceid
                        , lower(AA.commondata_7) as country
                        , to_char(AA.uniquetime_1,'YYYY-MM-DD') AS createtime
                        , 0 as paypoint
                    FROM [:GameName].bu1002 AA
                    WHERE 1 = 1
                        AND ( 1 != 1
                            OR (AA.dt >= '[:StartDateLine]' AND AA.dt <= '[:EndDateLine]')
                            OR (AA.dt >= '[:PreStartDateLine]' AND AA.dt <= '[:PreEndDateLine]')
                        )
                    UNION ALL
                    SELECT
                        '6002' AS dataname
                        , AA.dt
                        , AA.commondata_1 as serviceaccountid
                        , lower(AA.commondata_7) as country
                        , NULL AS createtime
                        , sum(AA.uniqueint_1) as  paypoint
                    FROM [:GameName].bu6002 AA
                    where 1 = 1
                        and AA.dt >= '[:StartDateLine]'
                        and AA.dt <= '[:EndDateLine]'
                    GROUP BY
                        AA.dt
                        , AA.commondata_1
                        , lower(AA.commondata_7)
                ) AAA
                WHERE 1 = 1
                GROUP BY
                    AAA.serviceid
            ), logintype_revenue_data as (
                SELECT
                    '[:DataReportName]' as dt
                    , 'all' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'step' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                    AND ( 1 != 1
                        OR (AAAA.nowlogin = 1 AND AAAA.prelogin = 1 )
                        OR (AAAA.nowlogin = 0 AND AAAA.paypoint != 0)
                    )
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'reback' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                    AND AAAA.nowlogin = 1
                    AND AAAA.prelogin = 0
                    AND AAAA.newlogin = 0
                UNION ALL
                SELECT
                    '[:DataReportName]' as dt
                    , 'newcount' AS logintype
                    , 'all' as country
                    , SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END) AS logincount
                    , SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END) AS paycount
                    , SUM(AAAA.paypoint) AS paypoint
                    , round(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1.0 ELSE 0 END)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as paidaccountrate
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.nowlogin != 0 THEN 1 ELSE 0 END),1),2) as loginaccountarpu
                    , round(SUM(AAAA.paypoint)/GREATEST(SUM(CASE WHEN AAAA.paypoint != 0 THEN 1 ELSE 0 END),1),2) as paidaccountarpu
                FROM basic_data AAAA
                WHERE 1 = 1
                    AND AAAA.nowlogin = 1
                    AND AAAA.prelogin = 0
                    AND AAAA.newlogin = 1
            ) INSERT INTO [:GameName].bureportstatistics
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'logincount' as datatype4
                , null as datatype5
                , AAAAA.logincount as value
            FROM logintype_revenue_data AAAAA	
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paycount' as datatype4
                , null as datatype5
                , AAAAA.paycount as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paypoint' as datatype4
                , null as datatype5
                , AAAAA.paypoint as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paidaccountrate' as datatype4
                , null as datatype5
                , AAAAA.paidaccountrate as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'loginaccountarpu' as datatype4
                , null as datatype5
                , AAAAA.loginaccountarpu as value
            FROM logintype_revenue_data AAAAA		
            UNION ALL
            SELECT
                dt::text as reportName
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'logintype_revenue_data' as datatype1
                , country as datatype2
                , logintype as datatype3
                , 'paidaccountarpu' as datatype4
                , null as datatype5
                , AAAAA.paidaccountarpu as value
            FROM logintype_revenue_data AAAAA ;
        """
        return "OrderInsert", [insertSQL]