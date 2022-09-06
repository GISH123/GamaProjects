import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import copy
import datetime
import pandas , numpy , random , math
from dotenv import load_dotenv
from scipy.spatial import distance
from package.common.database.hiveCtrl import HiveCtrl

class UseModel_LoginTag() :

    @classmethod
    def MakeUseModel_LoginTag_M0_1_1(self, modelInfo):
        makeTime = modelInfo['makeTime']
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        ye, no = 1, 0
        tagTextMaps = {}
        tagTextMaps['weekdays'] = [no, no, no, ye, ye, ye]  # 平日上線，假日不上線
        tagTextMaps['work'] = [no, no, ye, no, ye, ye]  # 平日晚上上線，假日早晚上線
        tagTextMaps['holiday'] = [ye, ye, ye, no, no, no]  # 平日不上線，假日上線
        tagTextMaps['night'] = [ye, no, ye, ye, no, ye]  # 白天不上線，晚上凌晨上線
        tagTextMaps['full'] = [ye, ye, ye, ye, ye, ye]  # 每天都上線

        # 向量長度
        def getVectorLength(vectorArray):
            return numpy.linalg.norm(vectorArray)

        # 向量內積
        def getInnerProduct(vectorArrayX, vectorArrayY):
            return numpy.dot(vectorArrayX, vectorArrayY)

        # 向量COSθ
        def getCOSTheta(vectorArrayX, vectorArrayY):
            innerProduct = getInnerProduct(vectorArrayX, vectorArrayY)
            xVectorLength = getVectorLength(vectorArrayX)
            yVectorLength = getVectorLength(vectorArrayY)
            return innerProduct / (xVectorLength * yVectorLength)

        # 基本參數處理 --------------------------------------------------

        # 各類別標準一：各類別的欄位各自定義相關數值
        originalTagTextMaps = tagTextMaps

        # 基本資料處理 --------------------------------------------------
        sql = """
           SELECT
               AA.product as product -- 產品名稱
               , AA.project as project -- 計畫名稱
               , AA.step as step -- 資料步驟
               , AA.version as version -- 資料版本
               , AA.dt as dt -- 資料時間
               , AA.commondata_001 as commondata_001 -- 服務帳號ID
               , AA.commondata_005 as commondata_005 -- 點數帳號ID
               , AA.commondata_006 as commondata_006 -- bf!APPOpenID
               , AA.uniquefloat_001 as uniquefloat_001 -- 平日凌晨
               , AA.uniquefloat_002 as uniquefloat_002 -- 平日白天
               , AA.uniquefloat_003 as uniquefloat_003 -- 平日晚上
               , AA.uniquefloat_004 as uniquefloat_004 -- 假日凌晨
               , AA.uniquefloat_005 as uniquefloat_005 -- 假日白天
               , AA.uniquefloat_006 as uniquefloat_006 -- 假日凌晨
           FROM gtwpd.model_usedata AA
           WHERE 1 = 1
               AND AA.product = 'maple'
               AND AA.project = 'LoginTag'
               AND AA.step = 'PreProcess'
               AND AA.version = 'P0_1_1'
               AND AA.dt = '[:DT]'
        """.replace("[:DT]",makeTime.replace("-",""))

        print(sql)

        originalDF = hiveCtrl.searchSQL(sql)
        # originalDF.to_csv("maple/LoginTag/file/tempdata/MakePreProcess_LoginTag_P0_1_1.csv", sep="\t", index=None)
        # originalDF = pandas.read_csv("maple/LoginTag/file/tempdata/MakePreProcess_LoginTag_P0_1_1.csv", sep="\t").sample(frac=0.01, replace=True, random_state=1)
        processDF = originalDF.drop(['product', 'project', 'step', 'version', 'dt', 'commondata_001', 'commondata_005', 'commondata_006'], axis=1)
        resultDF = pandas.DataFrame()
        processColumnArray = processDF.columns.to_list()

        # 各類別標準二：各類別的欄位各自定義相關高低標 --------------------------------------------------

        dataHighQuantile = []
        dataLowQuantile = []
        for columnName in processColumnArray:
            dataHighQuantile.append(numpy.quantile(processDF[columnName], 0.95))
            dataLowQuantile.append(numpy.quantile(processDF[columnName], 0.05))

        # 1 或 0 已經不能夠表達高標與低標採取高標的0.95與低標的0.05作為高低標標準

        preprocessTagTextMaps = {}

        for key in originalTagTextMaps.keys():
            originalTagTextArray = tagTextMaps[key]
            preprocessTagTextArray = []
            count = 0
            for tagValue in originalTagTextArray:
                preprocessTagTextArray.append((dataHighQuantile[count] - dataLowQuantile[count]) * tagValue + dataLowQuantile[count])
                count = count + 1
            preprocessTagTextMaps[key] = preprocessTagTextArray

        def timeOnlineEuclideanDistance(rowdata, targetPoint):
            currentlyPoint = []
            for columnName in processColumnArray:
                currentlyPoint.append(rowdata[columnName])
            compareDistance = distance.euclidean(currentlyPoint, targetPoint)
            return compareDistance

        # 算是每一個類別的歐式距離 --------------------------------------------------
        # 不用角度的原因是因為後面要使用排名前20%的加總為新的標準
        # 直接用角度的話會將上線時間很少的玩家列進來，這樣就失去相關的意義

        for key in preprocessTagTextMaps.keys():
            processDF[key + '_ED'] = processDF.apply(timeOnlineEuclideanDistance, targetPoint=preprocessTagTextMaps[key], axis=1)
        # 算是每一個類別的相關排名(歐式距離越近代表相似度越高) --------------------------------------------------

        for key in preprocessTagTextMaps.keys():
            processDF[key + '_ED_rank'] = processDF[key + '_ED'].rank(method='first', ascending=False) / len(processDF) * 100

        # 各欄位標準二：各類別排名前20%的人加總平均視為新的標準 --------------------------------------------------
        # 將各類別排名前20%的人加總平均視為新的標準 --------------------------------------------------

        for tagKey in preprocessTagTextMaps.keys():
            tagName = tagKey + '_ED_rank'

            df_mask = processDF[tagName] >= 80
            for compareKey in preprocessTagTextMaps.keys():
                compareName = compareKey + '_ED_rank'
                df_mask = df_mask & (processDF[tagName] >= processDF[compareName])

            preprocessTagTextMaps[tagKey] = []
            for columnName in originalDF.drop(['product', 'project', 'step', 'version', 'dt', 'commondata_001', 'commondata_005', 'commondata_006'], axis=1).columns:
                preprocessTagTextMaps[tagKey].append(numpy.mean(processDF[df_mask][columnName]))

        # 將新的標準使用COSTheta做波形相識程度 --------------------------------------------------

        def timeOnlineCOSTheta(rowdata, targetPoint):
            currentlyPoint = []
            for columnName in processColumnArray:
                currentlyPoint.append(rowdata[columnName])
            comparePointTheta = getCOSTheta(currentlyPoint, targetPoint)
            return comparePointTheta

        for key in tagTextMaps.keys():
            processDF[key + '_COST'] = processDF.apply(timeOnlineCOSTheta, targetPoint=preprocessTagTextMaps[key], axis=1)

        # 找出角度差異最小的視為該標籤 --------------------------------------------------
        # 這個階段是找出各類別相識的人的狀況與向量長度無關 --------------------------------------------------

        def timeOnlineCOSThetaRank(rowdata):
            type = '無'
            amount = -99
            for key in tagTextMaps.keys():
                if rowdata[key + '_COST'] > amount:
                    amount = rowdata[key + '_COST']
                    type = key
            return type

        processDF['tag'] = processDF.apply(timeOnlineCOSThetaRank, axis=1)

        def calculateTotal(rowdata):
            total = 0
            for columnName in processColumnArray:
                total = total + rowdata[columnName]
            return total

        processDF['total'] = processDF.apply(calculateTotal, axis=1)

        # --------------------------------------------------
        resultDF['commondata_001'] = originalDF['commondata_001']
        resultDF['commondata_005'] = originalDF['commondata_005']
        resultDF['commondata_006'] = originalDF['commondata_006']
        # resultDF['uniquefloat_001'] = originalDF['uniquefloat_001']
        # resultDF['uniquefloat_002'] = originalDF['uniquefloat_002']
        # resultDF['uniquefloat_003'] = originalDF['uniquefloat_003']
        # resultDF['uniquefloat_004'] = originalDF['uniquefloat_004']
        # resultDF['uniquefloat_005'] = originalDF['uniquefloat_005']
        # resultDF['uniquefloat_006'] = originalDF['uniquefloat_006']

        resultDF['uniquefloat_001'] = processDF['weekdays_COST']
        resultDF['uniquefloat_002'] = processDF['work_COST']
        resultDF['uniquefloat_003'] = processDF['holiday_COST']
        resultDF['uniquefloat_004'] = processDF['night_COST']
        resultDF['uniquefloat_005'] = processDF['full_COST']
        #resultDF['M0_0_1_Tag'] = processDF['tag']

        #resultDF.to_csv("maple/LoginTag/file/tempdata/MakePreProcess_LoginTag_M0_1_1.csv", sep="\t", index=False)
        #print(resultDF)
        #print(resultDF.groupby('M0_0_1_Tag').size())

        return "MakeUseModelFileInsert" , [resultDF,resultDF]

    @classmethod
    def MakeUseModel_LoginTag_M0_1_2(self, modelInfo):
        makeTime = modelInfo['makeTime']
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        ye, no = 1, 0
        tagTextMaps = {}
        tagTextMaps['weekdays'] = [no, no, no, ye, ye, ye]  # 平日上線，假日不上線
        tagTextMaps['work'] = [no, no, ye, no, ye, ye]  # 平日晚上上線，假日早晚上線
        tagTextMaps['holiday'] = [ye, ye, ye, no, no, no]  # 平日不上線，假日上線
        tagTextMaps['night'] = [ye, no, ye, ye, no, ye]  # 白天不上線，晚上凌晨上線
        tagTextMaps['full'] = [ye, ye, ye, ye, ye, ye]  # 每天都上線

        # 向量長度
        def getVectorLength(vectorArray):
            return numpy.linalg.norm(vectorArray)

        # 向量內積
        def getInnerProduct(vectorArrayX, vectorArrayY):
            return numpy.dot(vectorArrayX, vectorArrayY)

        # 向量COSθ
        def getCOSTheta(vectorArrayX, vectorArrayY):
            innerProduct = getInnerProduct(vectorArrayX, vectorArrayY)
            xVectorLength = getVectorLength(vectorArrayX)
            yVectorLength = getVectorLength(vectorArrayY)
            return innerProduct / (xVectorLength * yVectorLength)

        # 基本參數處理 --------------------------------------------------

        # 各類別標準一：各類別的欄位各自定義相關數值
        originalTagTextMaps = tagTextMaps

        # 基本資料處理 --------------------------------------------------
        sql = """
           SELECT
               AA.product as product -- 產品名稱
               , AA.project as project -- 計畫名稱
               , AA.step as step -- 資料步驟
               , AA.version as version -- 資料版本
               , AA.dt as dt -- 資料時間
               , AA.commondata_001 as commondata_001 -- 服務帳號ID
               , AA.commondata_005 as commondata_005 -- 點數帳號ID
               , AA.commondata_006 as commondata_006 -- bf!APPOpenID
               , AA.uniquefloat_001 as uniquefloat_001 -- 平日凌晨
               , AA.uniquefloat_002 as uniquefloat_002 -- 平日白天
               , AA.uniquefloat_003 as uniquefloat_003 -- 平日晚上
               , AA.uniquefloat_004 as uniquefloat_004 -- 假日凌晨
               , AA.uniquefloat_005 as uniquefloat_005 -- 假日白天
               , AA.uniquefloat_006 as uniquefloat_006 -- 假日凌晨
           FROM gtwpd.model_usedata AA
           WHERE 1 = 1
               AND AA.product = 'maple'
               AND AA.project = 'LoginTag'
               AND AA.step = 'PreProcess'
               AND AA.version = 'P0_1_2'
               AND AA.dt = '[:DT]'
        """.replace("[:DT]", makeTime.replace("-", ""))

        originalDF = hiveCtrl.searchSQL(sql)
        # originalDF.to_csv("maple/LoginTag/file/tempdata/MakePreProcess_LoginTag_P0_1_1.csv", sep="\t", index=None)
        # originalDF = pandas.read_csv("maple/LoginTag/file/tempdata/MakePreProcess_LoginTag_P0_1_1.csv", sep="\t").sample(frac=0.01, replace=True, random_state=1)
        processDF = originalDF.drop(['product', 'project', 'step', 'version', 'dt', 'commondata_001', 'commondata_005', 'commondata_006'], axis=1)
        resultDF = pandas.DataFrame()
        processColumnArray = processDF.columns.to_list()

        # 各類別標準二：各類別的欄位各自定義相關高低標 --------------------------------------------------

        dataHighQuantile = []
        dataLowQuantile = []
        for columnName in processColumnArray:
            dataHighQuantile.append(numpy.quantile(processDF[columnName], 0.95))
            dataLowQuantile.append(numpy.quantile(processDF[columnName], 0.05))

        # 1 或 0 已經不能夠表達高標與低標採取高標的0.95與低標的0.05作為高低標標準

        preprocessTagTextMaps = {}

        for key in originalTagTextMaps.keys():
            originalTagTextArray = tagTextMaps[key]
            preprocessTagTextArray = []
            count = 0
            for tagValue in originalTagTextArray:
                preprocessTagTextArray.append((dataHighQuantile[count] - dataLowQuantile[count]) * tagValue + dataLowQuantile[count])
                count = count + 1
            preprocessTagTextMaps[key] = preprocessTagTextArray

        def timeOnlineEuclideanDistance(rowdata, targetPoint):
            currentlyPoint = []
            for columnName in processColumnArray:
                currentlyPoint.append(rowdata[columnName])
            compareDistance = distance.euclidean(currentlyPoint, targetPoint)
            return compareDistance

        # 算是每一個類別的歐式距離 --------------------------------------------------
        # 不用角度的原因是因為後面要使用排名前20%的加總為新的標準
        # 直接用角度的話會將上線時間很少的玩家列進來，這樣就失去相關的意義

        for key in preprocessTagTextMaps.keys():
            processDF[key + '_ED'] = processDF.apply(timeOnlineEuclideanDistance, targetPoint=preprocessTagTextMaps[key], axis=1)
        # 算是每一個類別的相關排名(歐式距離越近代表相似度越高) --------------------------------------------------

        for key in preprocessTagTextMaps.keys():
            processDF[key + '_ED_rank'] = processDF[key + '_ED'].rank(method='first', ascending=False) / len(processDF) * 100

        # 各欄位標準二：各類別排名前20%的人加總平均視為新的標準 --------------------------------------------------
        # 將各類別排名前20%的人加總平均視為新的標準 --------------------------------------------------

        for tagKey in preprocessTagTextMaps.keys():
            tagName = tagKey + '_ED_rank'

            df_mask = processDF[tagName] >= 80
            for compareKey in preprocessTagTextMaps.keys():
                compareName = compareKey + '_ED_rank'
                df_mask = df_mask & (processDF[tagName] >= processDF[compareName])

            preprocessTagTextMaps[tagKey] = []
            for columnName in originalDF.drop(['product', 'project', 'step', 'version', 'dt', 'commondata_001', 'commondata_005', 'commondata_006'], axis=1).columns:
                preprocessTagTextMaps[tagKey].append(numpy.mean(processDF[df_mask][columnName]))

        # 將新的標準使用COSTheta做波形相識程度 --------------------------------------------------

        def timeOnlineCOSTheta(rowdata, targetPoint):
            currentlyPoint = []
            for columnName in processColumnArray:
                currentlyPoint.append(rowdata[columnName])
            comparePointTheta = getCOSTheta(currentlyPoint, targetPoint)
            return comparePointTheta

        for key in tagTextMaps.keys():
            processDF[key + '_COST'] = processDF.apply(timeOnlineCOSTheta, targetPoint=preprocessTagTextMaps[key], axis=1)

        # 找出角度差異最小的視為該標籤 --------------------------------------------------
        # 這個階段是找出各類別相識的人的狀況與向量長度無關 --------------------------------------------------

        def timeOnlineCOSThetaRank(rowdata):
            type = '無'
            amount = -99
            for key in tagTextMaps.keys():
                if rowdata[key + '_COST'] > amount:
                    amount = rowdata[key + '_COST']
                    type = key
            return type

        processDF['tag'] = processDF.apply(timeOnlineCOSThetaRank, axis=1)

        def calculateTotal(rowdata):
            total = 0
            for columnName in processColumnArray:
                total = total + rowdata[columnName]
            return total

        processDF['total'] = processDF.apply(calculateTotal, axis=1)

        # --------------------------------------------------
        resultDF['commondata_001'] = originalDF['commondata_001']
        resultDF['commondata_005'] = originalDF['commondata_005']
        resultDF['commondata_006'] = originalDF['commondata_006']
        # resultDF['uniquefloat_001'] = originalDF['uniquefloat_001']
        # resultDF['uniquefloat_002'] = originalDF['uniquefloat_002']
        # resultDF['uniquefloat_003'] = originalDF['uniquefloat_003']
        # resultDF['uniquefloat_004'] = originalDF['uniquefloat_004']
        # resultDF['uniquefloat_005'] = originalDF['uniquefloat_005']
        # resultDF['uniquefloat_006'] = originalDF['uniquefloat_006']

        resultDF['uniquefloat_001'] = processDF['weekdays_COST']
        resultDF['uniquefloat_002'] = processDF['work_COST']
        resultDF['uniquefloat_003'] = processDF['holiday_COST']
        resultDF['uniquefloat_004'] = processDF['night_COST']
        resultDF['uniquefloat_005'] = processDF['full_COST']
        # resultDF['M0_0_1_Tag'] = processDF['tag']

        # resultDF.to_csv("maple/LoginTag/file/tempdata/MakePreProcess_LoginTag_M0_1_1.csv", sep="\t", index=False)
        # print(resultDF)
        # print(resultDF.groupby('M0_0_1_Tag').size())

        return "MakeUseModelFileInsert", [resultDF, resultDF]

    @classmethod
    def MakeUseModel_LoginTag_M0_1_3(self, modelInfo):
        # 向量長度　輸入vectorArray 算出向量長度
        def getVectorLength(vectorArray):
            return numpy.linalg.norm(vectorArray)

        # 向量內積
        def getInnerProduct(vectorArrayX, vectorArrayY):
            return numpy.dot(vectorArrayX, vectorArrayY)

        # 向量COSθ 輸入vectorArrayX 與 vectorArrayY 算出兩個向量的　COSθ
        def getCOSTheta(vectorArrayX, vectorArrayY):
            innerProduct = getInnerProduct(vectorArrayX, vectorArrayY)
            xVectorLength = getVectorLength(vectorArrayX)
            yVectorLength = getVectorLength(vectorArrayY)
            return innerProduct / (xVectorLength * yVectorLength)

        makeTime = modelInfo['makeTime']
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        ye, no = 1, 0
        tagTextMaps = {}
        tagTextMaps['weekdays'] = [no, no, no, ye, ye, ye]  # 平日上線，假日不上線
        tagTextMaps['work'] = [no, no, ye, no, ye, ye]      # 平日晚上上線，假日早晚上線
        tagTextMaps['holiday'] = [ye, ye, ye, no, no, no]   # 平日不上線，假日上線
        tagTextMaps['night'] = [ye, no, ye, ye, no, ye]     # 白天不上線，晚上凌晨上線
        tagTextMaps['full'] = [ye, ye, ye, ye, ye, ye]      # 每天都上線

        # 基本參數處理 --------------------------------------------------

        # 各類別標準一：各類別的欄位各自定義相關數值
        originalTagTextMaps = tagTextMaps

        # 基本資料處理 --------------------------------------------------
        sql = """
           SELECT
               AA.product as product -- 產品名稱
               , AA.project as project -- 計畫名稱
               , AA.step as step -- 資料步驟
               , AA.version as version -- 資料版本
               , AA.dt as dt -- 資料時間
               , AA.commondata_001 as commondata_001 -- 服務帳號ID
               , AA.commondata_005 as commondata_005 -- 點數帳號ID
               , AA.commondata_006 as commondata_006 -- bf!APPOpenID
               , AA.uniquefloat_001 as uniquefloat_001 -- 平日凌晨
               , AA.uniquefloat_002 as uniquefloat_002 -- 平日白天
               , AA.uniquefloat_003 as uniquefloat_003 -- 平日晚上
               , AA.uniquefloat_004 as uniquefloat_004 -- 假日凌晨
               , AA.uniquefloat_005 as uniquefloat_005 -- 假日白天
               , AA.uniquefloat_006 as uniquefloat_006 -- 假日凌晨
           FROM gtwpd.model_usedata AA
           WHERE 1 = 1
               AND AA.product = 'maple'
               AND AA.project = 'LoginTag'
               AND AA.step = 'PreProcess'
               AND AA.version = 'P0_1_2'
               AND AA.dt = '[:DT]'
        """.replace("[:DT]", makeTime.replace("-", ""))

        originalDF = hiveCtrl.searchSQL(sql)
        processDF = originalDF.drop(['product', 'project', 'step', 'version', 'dt', 'commondata_001', 'commondata_005', 'commondata_006'], axis=1)
        resultDF = pandas.DataFrame()
        processColumnArray = processDF.columns.to_list()

        # 各類別標準二：各類別的欄位各自定義相關高低標 --------------------------------------------------

        dataHighQuantile = []
        dataLowQuantile = []
        for columnName in processColumnArray:
            dataHighQuantile.append(numpy.quantile(processDF[columnName], 0.95))
            dataLowQuantile.append(numpy.quantile(processDF[columnName], 0.05))

        # 1 或 0 已經不能夠表達高標與低標採取高標的0.95與低標的0.05作為高低標標準

        preprocessTagTextMaps = {}

        for key in originalTagTextMaps.keys():
            originalTagTextArray = tagTextMaps[key]
            preprocessTagTextArray = []
            count = 0
            for tagValue in originalTagTextArray:
                timeValue = (dataHighQuantile[count] - dataLowQuantile[count]) * tagValue + dataLowQuantile[count]
                timeValue = round(timeValue,1)
                preprocessTagTextArray.append(timeValue)
                count = count + 1
            preprocessTagTextMaps[key] = preprocessTagTextArray

        for key in preprocessTagTextMaps.keys():
            print("{} {}".format(key , preprocessTagTextMaps[key]))

        # 算是每一個類別的歐式距離 --------------------------------------------------
        # 不用角度的原因是因為後面要使用排名前20%的加總為新的標準
        # 直接用角度的話會將上線時間很少的玩家列進來，這樣就失去相關的意義

        def timeOnlineEuclideanDistance(rowdata, targetPoint):
            currentlyPoint = []
            for columnName in processColumnArray:
                currentlyPoint.append(rowdata[columnName])
            compareDistance = distance.euclidean(currentlyPoint, targetPoint)
            return compareDistance

        for key in preprocessTagTextMaps.keys():
            processDF[key + '_ED'] = processDF.apply(timeOnlineEuclideanDistance, targetPoint=preprocessTagTextMaps[key], axis=1)
        # 算是每一個類別的相關排名(歐式距離越近代表相似度越高) --------------------------------------------------

        for key in preprocessTagTextMaps.keys():
            processDF[key + '_ED_rank'] = processDF[key + '_ED'].rank(method='first', ascending=False) / len(processDF) * 100

        # 各欄位標準二：各類別排名前20%的人加總平均視為新的標準 --------------------------------------------------
        # 將各類別排名前20%的人加總平均視為新的標準 --------------------------------------------------

        for tagKey in preprocessTagTextMaps.keys():
            tagName = tagKey + '_ED_rank'
            otherlist = list(preprocessTagTextMaps.keys())
            otherlist.remove(tagKey)
            # df_mask = (processDF[tagName] >= 90) & (processDF[otherlist[0]+'_ED_rank'] <= 50) & (processDF[otherlist[1]+'_ED_rank'] <= 50) & (processDF[otherlist[2]+'_ED_rank'] <= 50) & (processDF[otherlist[3]+'_ED_rank'] <= 50)
            df_mask = (processDF[tagName] >= 80) \
                      & (processDF[otherlist[0]+'_ED_rank'] + 0.1 <= processDF[tagName]) \
                      & (processDF[otherlist[1]+'_ED_rank'] + 0.1 <= processDF[tagName]) \
                      & (processDF[otherlist[2]+'_ED_rank'] + 0.1 <= processDF[tagName]) \
                      & (processDF[otherlist[3]+'_ED_rank'] + 0.1 <= processDF[tagName])
            for compareKey in preprocessTagTextMaps.keys():
                compareName = compareKey + '_ED_rank'
                df_mask = df_mask & (processDF[tagName] >= processDF[compareName])

            preprocessTagTextMaps[tagKey] = []
            for columnName in originalDF.drop(['product', 'project', 'step', 'version', 'dt', 'commondata_001', 'commondata_005', 'commondata_006'], axis=1).columns:
                preprocessTagTextMaps[tagKey].append(round(numpy.mean(processDF[df_mask][columnName]),2))

        for key in preprocessTagTextMaps.keys():
            print("{} {}".format(key, preprocessTagTextMaps[key]))

        # 將新的標準使用COSTheta做波形相識程度 --------------------------------------------------

        def timeOnlineCOSTheta(rowdata, targetPoint):
            currentlyPoint = []
            for columnName in processColumnArray:
                currentlyPoint.append(rowdata[columnName])
            comparePointTheta = getCOSTheta(currentlyPoint, targetPoint)
            return comparePointTheta

        for key in tagTextMaps.keys():
            processDF[key + '_COST'] = processDF.apply(timeOnlineCOSTheta, targetPoint=preprocessTagTextMaps[key], axis=1)

        # 找出角度差異最小的視為該標籤 --------------------------------------------------
        # 這個階段是找出各類別相識的人的狀況與向量長度無關 --------------------------------------------------

        def timeOnlineCOSThetaRank(rowdata):
            type = '無'
            amount = -99
            for key in tagTextMaps.keys():
                if rowdata[key + '_COST'] > amount:
                    amount = rowdata[key + '_COST']
                    type = key
            return type

        processDF['tag'] = processDF.apply(timeOnlineCOSThetaRank, axis=1)

        def calculateTotal(rowdata):
            total = 0
            for columnName in processColumnArray:
                total = total + rowdata[columnName]
            return total

        processDF['total'] = processDF.apply(calculateTotal, axis=1)

        # --------------------------------------------------
        resultDF['commondata_001'] = originalDF['commondata_001']
        resultDF['commondata_005'] = originalDF['commondata_005']
        resultDF['commondata_006'] = originalDF['commondata_006']
        resultDF['uniquefloat_001'] = processDF['weekdays_COST']
        resultDF['uniquefloat_002'] = processDF['work_COST']
        resultDF['uniquefloat_003'] = processDF['holiday_COST']
        resultDF['uniquefloat_004'] = processDF['night_COST']
        resultDF['uniquefloat_005'] = processDF['full_COST']

        return "MakeUseModelFileInsert", [resultDF, resultDF]
