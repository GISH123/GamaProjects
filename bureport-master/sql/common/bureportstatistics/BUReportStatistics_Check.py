
class BUReportStatistics_Check() :

    @classmethod
    def make901DataSQL(self, makeInfo):
        insertSQL = """
            INSERT INTO [:GameName].bureportstatistics
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1001' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1001 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1002' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1002 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1003' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1003 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1102' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1102 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1103' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1103 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1131' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1131 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1132' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1132 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1133' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1133 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1134' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1134 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1135' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1135 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1136' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1136 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1137' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1137 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd1904' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu1904 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'game' as datatype2
                , 'd3002' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM [:GameName].bu3002 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
            UNION ALL
            SELECT
                '[:DataReportName]' as datatime
                , '[:StartDateLine]'::date as startdate
                , '[:EndDateLine]'::date as enddate
                , '[:PeriodType]' as periodType
                , '[:GameName]' as gamename
                , '[:ReportCode]'::text as reportcode
                , 'check_data' as datatype1
                , 'bf' as datatype2
                , 'd3002' as datatype3
                , null as datatype4
                , null as datatype5
                , count(*) as value
            FROM bf.d3002 aa
            WHERE 1 = 1
                and aa.dt >= '[:StartDateLine]'
                and aa.dt <= '[:EndDateLine]'
                AND AA.commondata_10 = '[:GameName]';
        """
        return "OrderInsert", [insertSQL]

