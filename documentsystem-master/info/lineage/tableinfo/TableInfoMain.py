from info.lineage.tableinfo.TableInfo_BUReportStatistics import TableInfo_BUReportStatistics
from info.lineage.tableinfo.TableInfo_1000 import TableInfo_1000
from info.lineage.tableinfo.TableInfo_1100 import TableInfo_1100
from info.lineage.tableinfo.TableInfo_1800 import TableInfo_1800
from info.lineage.tableinfo.TableInfo_2000 import TableInfo_2000
from info.lineage.tableinfo.TableInfo_2100 import TableInfo_2100
from info.lineage.tableinfo.TableInfo_3000 import TableInfo_3000
from info.lineage.tableinfo.TableInfo_5000 import TableInfo_5000
from info.lineage.tableinfo.TableInfo_5100 import TableInfo_5100
from info.lineage.tableinfo.TableInfo_6000 import TableInfo_6000
from info.lineage.tableinfo.TableInfo_6500 import TableInfo_6500
from info.lineage.tableinfo.TableInfo_6600 import TableInfo_6600
from info.lineage.tableinfo.TableInfo_10000 import TableInfo_10000
from info.lineage.tableinfo.TableInfo_11000 import TableInfo_11000


class TableInfoMain(
        TableInfo_BUReportStatistics
        , TableInfo_1000
        , TableInfo_1100
        , TableInfo_1800
        , TableInfo_2000
        , TableInfo_2100
        , TableInfo_3000
        , TableInfo_5000
        , TableInfo_5100
        , TableInfo_6000
        , TableInfo_6500
        , TableInfo_6600
        , TableInfo_10000
        , TableInfo_11000
    ) :
    def __init__(self):
        pass

