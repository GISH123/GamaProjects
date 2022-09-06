import pandas

class PreProcess_P99Test() :

    @classmethod
    def MakePreProcess_P99Test_P0_1902_1(self, makeInfo):
        from common.AutoTag.PreProcessFunction.PreProcessFunction import PreProcessFunction
        preProcessFunction = PreProcessFunction()
        orderSQL = preProcessFunction.getQuantileTextExcludeZeroSQLByPercentRank()
        orderSQL = orderSQL.replace("[:SourseDataVersion]","[:RawDataVersion]")
        orderSQL = orderSQL.replace("[:TargetDataVersion]","[:PreProcessVersion]")
        return "MakePreProcessOrderSQLInsert", [orderSQL] , {}
