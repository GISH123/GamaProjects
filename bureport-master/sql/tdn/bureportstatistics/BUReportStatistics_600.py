from sql.common.bureportstatistics.BUReportStatistics_600 import BUReportStatistics_600 as BUReportStatistics_600_Common

class BUReportStatistics_600(BUReportStatistics_600_Common) :
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

    @classmethod
    def make6610DataSQL(self, makeInfo):
        insertSQL = """
            delete from [:GameName].bu6610
            where dt = '[:StartDateLine]'; 
            with last_login_date as (
                select 
                    commondata_2
                    , max(dt):: date as last_login_date
                from [:GameName].bu1003
                where uniqueint_1 > 0
                and dt <= '[:StartDateLine]'
                group by 1
            ),
            max_register_date as (
                select 
                    commondata_2
                    , UniqueTime_1 as register_date
                from [:GameName].bu1002
                where UniqueTime_1 is not null
                and dt <= '[:StartDateLine]'
                and UniqueTime_1 >= '2019-09-01'
            ),
            login_data as (
                select distinct
                    a.commondata_2
                    , register_date
                    , last_login_date
                    , DATE_PART('day', last_login_date - register_date) as login_range
                from max_register_date as a
                left join last_login_date as b
                on a.commondata_2 = b.commondata_2
            ),
            paid_data as (
                select 
                    commondata_2
                    , min(dt) as first_paid_date
                    , max(dt) as latest_paid_date
                    , sum(uniquedbl_1) as paid_sum
                    , count(uniquedbl_1) as purchase_count
                    , sum(uniquedbl_1)/count(uniquedbl_1) as abs
                from [:GameName].bu6019
                where dt <= '[:StartDateLine]'
                group by commondata_2
            ),
            paid_data_per_month as (
                select commondata_2, avg(purchase_count) as avg_purchase_count
                from (
                    select 
                        commondata_2
                        , date_trunc('month', dt::date) as month 
                        , count(uniquedbl_1) as purchase_count
                    from [:GameName].bu6019
                    where dt <= '[:StartDateLine]'
                    group by commondata_2, date_trunc('month', dt::date)
                ) as tmp
                group by commondata_2
            ),
            lifetime_in_month as (
                select AVG(DATE_PART('day', last_login_date - register_date))/30 as lifetime_in_month
                from login_data
            )
            insert into [:GameName].bu6610
            select
                '[:StartDateLine]' as dt
                , a.commondata_2 as accountid
                , register_date:: date
                , last_login_date:: date
                , login_range
                , case 
                    when abs * avg_purchase_count * lifetime_in_month >= 1 and abs * avg_purchase_count * lifetime_in_month < 1000  then '1-1000'
                    when abs * avg_purchase_count * lifetime_in_month >= 1000 and abs * avg_purchase_count * lifetime_in_month < 2000  then '1000-2000'
                    when abs * avg_purchase_count * lifetime_in_month >= 2000 and abs * avg_purchase_count * lifetime_in_month < 3000  then '2000-3000'
                    when abs * avg_purchase_count * lifetime_in_month >= 3000 and abs * avg_purchase_count * lifetime_in_month < 4000  then '3000-4000'
                    when abs * avg_purchase_count * lifetime_in_month >= 4000 and abs * avg_purchase_count * lifetime_in_month < 5000  then '4000-5000'
                    when abs * avg_purchase_count * lifetime_in_month >= 5000 and abs * avg_purchase_count * lifetime_in_month < 6000  then '5000-6000'
                    when abs * avg_purchase_count * lifetime_in_month >= 6000 and abs * avg_purchase_count * lifetime_in_month < 7000  then '6000-7000'
                    when abs * avg_purchase_count * lifetime_in_month >= 6000 then '>7000'
                    else '0'
                end as ltv_category
                , first_paid_date
                , latest_paid_date
                , case when paid_sum is null then 0 else paid_sum end as paid_sum
                , purchase_count
                , abs
                , avg_purchase_count
                , lifetime_in_month
                , abs * avg_purchase_count * lifetime_in_month as ltv
            from login_data as a
            left join paid_data as b
            on a.commondata_2 = b.commondata_2
            left join paid_data_per_month as c
            on a.commondata_2 = c.commondata_2
            cross join lifetime_in_month
            order by paid_sum desc ;
        """
        return "OrderInsert", [insertSQL]

    @classmethod
    def make6611DataSQL(self, makeInfo):
        insertSQL = """
            delete from [:GameName].bu6611
            where dt = '[:StartDateLine]'; 
            with rfm_info as (
                select 
                    dt
                    , accountid
                    , ltv as monetary
                    , current_date - latest_paid_date::date as recency
                    , case when ltv is not null then purchase_count else null end as frequency
                from [:GameName].bu6610
                where dt = '[:StartDateLine]'
            ),
            rfm_dist_info as (
                select
                    percentile_disc(0.5) within group (order by rfm_info.monetary desc) as m
                    , percentile_disc(0.5) within group (order by rfm_info.recency) as r
                    , percentile_disc(0.5) within group (order by rfm_info.frequency desc) as f
                from rfm_info
            ),
            tmp as (
                select distinct
                    dt
                    , accountid
                    , monetary, recency, frequency
                    , m, r, f
                    , case 
                        when monetary >= m and recency < r and frequency >= f then 'high R, high F, high M'
                        when monetary < m and recency >= r and frequency < f then 'low R, low F, low M'
                        when monetary < m and recency < r and frequency >= f then 'low R, high F, high M'
                        when monetary >= m and recency >= r and frequency >= f then 'high R, low F, high M'
                        when monetary >= m and recency < r and frequency < f then 'high R, high F, low M'
                        when monetary < m and recency >= r and frequency >= f then 'low R, high F, low M'
                        when monetary >= m and recency >= r and frequency < f then 'high R, low F, high M'
                        when monetary < m and recency < r and frequency < f then 'low R, low F, high M'
                        when monetary is null then 'Non paid'
                    end as classification
                from rfm_info
                cross join rfm_dist_info
        )
        insert into [:GameName].bu6611
        select
            *
            , case 
                when classification = 'high R, high F, high M' then 'VIP 用戶'
                when classification = 'low R, low F, low M' then '沉睡用戶'
                when classification = 'high R, low F, high M' then '潛在 VIP 用戶'
                when classification = 'low R, high F, high M' then '流失 VIP 用戶'
                when classification = 'Non paid' then '非付費用戶'
            end as user_tag
        from tmp;
        """
        return "OrderInsert", [insertSQL]