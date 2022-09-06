import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from ExampleProduct.P05DataCheckAlarmExample.info.RawDataInfo_P05DataCheckAlarmExample import RawDataInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.PreProcessInfo_P05DataCheckAlarmExample import PreProcessInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.UseModelInfo_P05DataCheckAlarmExample import UseModelInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.ModelResultInfo_P05DataCheckAlarmExample import ModelResultInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.ModelScoreInfo_P05DataCheckAlarmExample import ModelScoreInfo_P05DataCheckAlarmExample
from package.modeldocument.documentCtrl import DocumentCtrl

# 請參考 P05DataCheckAlarmExample

rawdataInfo_P05DataCheckAlarmExample = RawDataInfo_P05DataCheckAlarmExample()
preprocessInfo_P05DataCheckAlarmExample = PreProcessInfo_P05DataCheckAlarmExample()
usemodelInfo_P05DataCheckAlarmExample = UseModelInfo_P05DataCheckAlarmExample()
modelresultInfo_P05DataCheckAlarmExample = ModelResultInfo_P05DataCheckAlarmExample()
modelscoreInfo_P05DataCheckAlarmExample = ModelScoreInfo_P05DataCheckAlarmExample()
documentCtrl = DocumentCtrl()

# 請參考 P02AdvancedExample

if __name__ == "__main__":
    dataMap = {
        "AllData" : [
            rawdataInfo_P05DataCheckAlarmExample.getRawDataInfo_P05DataCheckAlarmExample_R1_0_3()[1]
            , preprocessInfo_P05DataCheckAlarmExample.getPreProcessInfo_P05DataCheckAlarmExample_P1_0_3()[1]
            , usemodelInfo_P05DataCheckAlarmExample.getUseModelInfo_P05DataCheckAlarmExample_M1_0_3()[1]
            , modelresultInfo_P05DataCheckAlarmExample.getModelResultInfo_P05DataCheckAlarmExample_V0_0_4()[1]
            , modelscoreInfo_P05DataCheckAlarmExample.getModelScoreInfo_P05DataCheckAlarmExample_V0_0_4()[1]
        ]
        , "ModelResult" : [
            modelresultInfo_P05DataCheckAlarmExample.getModelResultInfo_P05DataCheckAlarmExample_V0_0_4()[1]
        ]
        , "ModelScore" : [
            modelscoreInfo_P05DataCheckAlarmExample.getModelScoreInfo_P05DataCheckAlarmExample_V0_0_4()[1]
        ]
        , "UseModel" : [
            usemodelInfo_P05DataCheckAlarmExample.getUseModelInfo_P05DataCheckAlarmExample_M1_0_3()[1]
        ]
        , "PreProcess": [
            preprocessInfo_P05DataCheckAlarmExample.getPreProcessInfo_P05DataCheckAlarmExample_P1_0_3()[1]
        ]
        , "RawData": [
            rawdataInfo_P05DataCheckAlarmExample.getRawDataInfo_P05DataCheckAlarmExample_R1_0_3()[1]
        ]
    }
    fileName = "ExampleProduct_P05DataCheckAlarmExample_ModelUseDataDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataDocInit.xlsx'
    outFilePath = 'ExampleProduct/P05DataCheckAlarmExample/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataDoc(dataMap,initFilePath,outFilePath)

    fileName = "ExampleProduct_P05DataCheckAlarmExample_ModelUseDataOmitDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataOmitDocInit.xlsx'
    outFilePath = 'ExampleProduct/P05DataCheckAlarmExample/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataOmitDoc(dataMap, initFilePath, outFilePath)


