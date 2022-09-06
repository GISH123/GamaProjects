from package.modeling.common.common.CommonFunction import CommonFunction

class RawDataFunction(CommonFunction):

    def __init__(self):
        pass

    def makeSampleUseModelDataSQL(self, makeInfo):
        sqlReplaceArr = makeInfo["SQLReplace"]
        columnInfoArr = makeInfo["ColumnInfo"]

        dataColumnArr = self.getUseModelColumnNameArr()

        columnStrList = []
        for dataColumn in dataColumnArr:
            oriColumn = "null"
            instColumn = dataColumn
            for columnInfo in columnInfoArr:
                if columnInfo[0] == dataColumn:
                    oriColumn = columnInfo[1]
            columnStr = "{} as {}".format(oriColumn, instColumn)
            columnStrList.append(columnStr)
        columnsStr = sqlReplaceArr.append(["[:DataColumn]",'\n                , '.join(columnStrList)])
        sampleSQL = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION (product = '[:ProductName]' , project = '[:Project]' ,version = '[:RawDataVersion]' , dt = '[:DateNoLine]' , step = 'RawData')
            SELECT 
                [:DataColumn]
            FROM gtwpd.model_usedata AA
            WHERE 1 = 1
                AND AA.game= '[:Game]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '[:TableNumber]' ; 
        """

        for sqlReplace in sqlReplaceArr :
            sampleSQL = sampleSQL.replace(sqlReplace[0],sqlReplace[1])

        return sampleSQL


