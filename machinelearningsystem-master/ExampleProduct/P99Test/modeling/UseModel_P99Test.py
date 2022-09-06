import copy


class UseModel_P99Test() :

    @classmethod
    def MakeUseModel_P99Test_M0_1902_1(self, modelInfo):
        makeInfo = copy.deepcopy(modelInfo)
        textInfo = {}
        makeInfo['textInfo'] = textInfo
        makeInfo['CompareColumn'] = "uniquefloat_001"
        textInfo['TextProduct'] = makeInfo['productName']
        textInfo['TextProject'] = makeInfo['project']
        textInfo['TextVersion'] =  'R0_1902_0'
        textInfo['TextStep'] =  'RawData'
        textInfo['TextDateNoLine'] =  makeInfo['makeTime'].replace("-","")

        orderSQLArr = []

        from common.AutoTag.CommonFunstion.CommonFunction import CommonFunction
        sqlArr = CommonFunction.getLimitTextSQL(makeInfo)

        for orderSQL in sqlArr :
            orderSQL = orderSQL.replace("[:TargetVersion]","[:PreProcessVersion]")
            orderSQL = orderSQL.replace("[:SourceVersion]","[:UseModelVersion]")
            orderSQLArr.append(orderSQL)
        print(orderSQL)
        return "MakeUseModelOrderSQLInsert",[] ,{}
