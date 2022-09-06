from package.modeling.common.rawdatafunction.RawDataFunction import RawDataFunction

class RawData_P99Test() :

    @classmethod
    def MakeRawData_P99Test_R0_1902_0(self, makeInfo):
        # df = ""
        from common.AutoTag.RawDataFunction.RawTextFunction import RawTextFunction
        rawTextFunction = RawTextFunction()
        df = rawTextFunction.MakeQuantileTextExcludeZeroDF()
        return "MakeRawDataFileInsertOverwrite", df , {}


    @classmethod
    def MakeRawData_P99Test_R0_1902_1(self, makeInfo) :
        sqlInfo = {
            "SQLReplace": [
                ["[:Game]", "lineagem"]
                , ["[:World]", "COMMON"]
                , ["[:TableNumber]", "11902"]
            ]
            , "ColumnInfo": [
                ["commondata_001", "commondata_1"]
                , ["commondata_002", "commondata_2"]
                , ["commondata_003", "commondata_3"]
                , ["commondata_004", "commondata_4"]
                , ["commondata_005", "commondata_5"]
                , ["commondata_006", "commondata_6"]
                , ["uniquefloat_001", "uniquedbl_2"]
            ]
        }
        rawDataFunction = RawDataFunction()
        orderSQL = rawDataFunction.makeSampleUseModelDataSQL(sqlInfo)
        return "MakeRawDataOrderSQLInsert", [orderSQL], {}
