import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from ExampleProduct.P03DocumentExample.info.RawDataInfo_P03DocumentExample import RawDataInfo_P03DocumentExample
from ExampleProduct.P03DocumentExample.info.PreProcessInfo_P03DocumentExample import PreProcessInfo_P03DocumentExample
from ExampleProduct.P03DocumentExample.info.UseModelInfo_P03DocumentExample import UseModelInfo_P03DocumentExample
from ExampleProduct.P03DocumentExample.info.ModelResultInfo_P03DocumentExample import ModelResultInfo_P03DocumentExample
from ExampleProduct.P03DocumentExample.info.ModelScoreInfo_P03DocumentExample import ModelScoreInfo_P03DocumentExample
from package.modeldocument.documentCtrl import DocumentCtrl

# 利用該程式製作相關資料文件，匯入到 XLS
# 該名稱以下 Sheet 名稱為 AllData、ModelResult、ModelScore、UseModel、PreProcess、RawData
#      以下為相關範例
#      dataMap = {
#         "[:Sheet名稱]" : [
#             [:資料集文件01]
#             , [:資料集文件02]
#             , [:資料集文件03]
#         ]
#     }
# 完成後匯出成 XLS

rawdataInfo_P03DocumentExample = RawDataInfo_P03DocumentExample()
preprocessInfo_P03DocumentExample = PreProcessInfo_P03DocumentExample()
usemodelInfo_P03DocumentExample = UseModelInfo_P03DocumentExample()
modelresultInfo_P03DocumentExample = ModelResultInfo_P03DocumentExample()
modelscoreInfo_P03DocumentExample = ModelScoreInfo_P03DocumentExample()
documentCtrl = DocumentCtrl()

if __name__ == "__main__":
    dataMap = {
        "AllData" : [
            rawdataInfo_P03DocumentExample.getRawDataInfo_P03DocumentExample_R1_0_3()[1]
            , preprocessInfo_P03DocumentExample.getPreProcessInfo_P03DocumentExample_P1_0_3()[1]
            , usemodelInfo_P03DocumentExample.getUseModelInfo_P03DocumentExample_M1_0_3()[1]
            , modelresultInfo_P03DocumentExample.getModelResultInfo_P03DocumentExample_V0_0_1()[1]
            , modelscoreInfo_P03DocumentExample.getModelScoreInfo_P03DocumentExample_V0_0_1()[1]
        ]
        , "ModelResult" : [
            modelresultInfo_P03DocumentExample.getModelResultInfo_P03DocumentExample_V0_0_1()[1]
        ]
        , "ModelScore" : [
            modelscoreInfo_P03DocumentExample.getModelScoreInfo_P03DocumentExample_V0_0_1()[1]
        ]
        , "UseModel" : [
            usemodelInfo_P03DocumentExample.getUseModelInfo_P03DocumentExample_M1_0_3()[1]
        ]
        , "PreProcess": [
            preprocessInfo_P03DocumentExample.getPreProcessInfo_P03DocumentExample_P1_0_3()[1]
        ]
        , "RawData": [
            rawdataInfo_P03DocumentExample.getRawDataInfo_P03DocumentExample_R1_0_3()[1]
        ]
    }
    fileName = "ExampleProduct_P03DocumentExample_ModelUseDataDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataDocInit.xlsx'
    outFilePath = 'ExampleProduct/P03DocumentExample/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataDoc(dataMap,initFilePath,outFilePath)

    fileName = "ExampleProduct_P03DocumentExample_ModelUseDataOmitDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataOmitDocInit.xlsx'
    outFilePath = 'ExampleProduct/P03DocumentExample/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataOmitDoc(dataMap, initFilePath, outFilePath)


