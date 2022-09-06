from sql.common.bureportstatistics.BUReportStatistics_100 import BUReportStatistics_100 as BUReportStatistics_100_Common

class BUReportStatistics_100(BUReportStatistics_100_Common) :

    @classmethod
    def make101DataSQL(self, makeInfo):
        reportType, reportSQLArray = super(). make101DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType , reportSQLArray

    @classmethod
    def make102DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make102DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make103DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make103DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make106DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make106DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make107DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make107DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make108DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make108DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make111DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make111DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make112DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make112DataSQL(makeInfo)
        deleteSQLStr = """
            DELETE
            FROM [:GameName].bureportstatistics
            WHERE 1 = 1 
                AND reportName = '[:DataReportName]' 
                AND datatype2 in ('tw','hk') ; 
        """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make113DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make113DataSQL(makeInfo)
        deleteSQLStr = """
           DELETE
           FROM [:GameName].bureportstatistics
           WHERE 1 = 1 
               AND reportName = '[:DataReportName]' 
               AND datatype2 in ('tw','hk') ; 
       """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray

    @classmethod
    def make114DataSQL(self, makeInfo):
        reportType, reportSQLArray = super().make114DataSQL(makeInfo)
        deleteSQLStr = """
           DELETE
           FROM [:GameName].bureportstatistics
           WHERE 1 = 1 
               AND reportName = '[:DataReportName]' 
               AND datatype2 in ('tw','hk') ; 
       """
        reportSQLArray.append(deleteSQLStr)
        return reportType, reportSQLArray