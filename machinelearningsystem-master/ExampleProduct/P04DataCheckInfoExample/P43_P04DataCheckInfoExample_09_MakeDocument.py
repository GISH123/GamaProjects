import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from ExampleProduct.P04DataCheckInfoExample.info.RawDataInfo_P04DataCheckInfoExample import RawDataInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.PreProcessInfo_P04DataCheckInfoExample import PreProcessInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.UseModelInfo_P04DataCheckInfoExample import UseModelInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.ModelResultInfo_P04DataCheckInfoExample import ModelResultInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.ModelScoreInfo_P04DataCheckInfoExample import ModelScoreInfo_P04DataCheckInfoExample
from package.modeldocument.documentCtrl import DocumentCtrl

# 請參考 P04DataCheckInfoExample

rawdataInfo_P04DataCheckInfoExample = RawDataInfo_P04DataCheckInfoExample()
preprocessInfo_P04DataCheckInfoExample = PreProcessInfo_P04DataCheckInfoExample()
usemodelInfo_P04DataCheckInfoExample = UseModelInfo_P04DataCheckInfoExample()
modelresultInfo_P04DataCheckInfoExample = ModelResultInfo_P04DataCheckInfoExample()
modelscoreInfo_P04DataCheckInfoExample = ModelScoreInfo_P04DataCheckInfoExample()
documentCtrl = DocumentCtrl()

# 請參考 P02AdvancedExample

if __name__ == "__main__":
    dataMap = {
        "AllData" : [
            rawdataInfo_P04DataCheckInfoExample.getRawDataInfo_P04DataCheckInfoExample_R1_0_3()[1]
            , preprocessInfo_P04DataCheckInfoExample.getPreProcessInfo_P04DataCheckInfoExample_P1_0_3()[1]
            , usemodelInfo_P04DataCheckInfoExample.getUseModelInfo_P04DataCheckInfoExample_M1_0_3()[1]
            , modelresultInfo_P04DataCheckInfoExample.getModelResultInfo_P04DataCheckInfoExample_V0_0_3()[1]
            , modelscoreInfo_P04DataCheckInfoExample.getModelScoreInfo_P04DataCheckInfoExample_V0_0_3()[1]
        ]
        , "ModelResult" : [
            modelresultInfo_P04DataCheckInfoExample.getModelResultInfo_P04DataCheckInfoExample_V0_0_3()[1]
        ]
        , "ModelScore" : [
            modelscoreInfo_P04DataCheckInfoExample.getModelScoreInfo_P04DataCheckInfoExample_V0_0_3()[1]
        ]
        , "UseModel" : [
            usemodelInfo_P04DataCheckInfoExample.getUseModelInfo_P04DataCheckInfoExample_M1_0_3()[1]
        ]
        , "PreProcess": [
            preprocessInfo_P04DataCheckInfoExample.getPreProcessInfo_P04DataCheckInfoExample_P1_0_3()[1]
        ]
        , "RawData": [
            rawdataInfo_P04DataCheckInfoExample.getRawDataInfo_P04DataCheckInfoExample_R1_0_3()[1]
        ]
    }
    fileName = "ExampleProduct_P04DataCheckInfoExample_ModelUseDataDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataDocInit.xlsx'
    outFilePath = 'ExampleProduct/P04DataCheckInfoExample/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataDoc(dataMap,initFilePath,outFilePath)

    fileName = "ExampleProduct_P04DataCheckInfoExample_ModelUseDataOmitDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataOmitDocInit.xlsx'
    outFilePath = 'ExampleProduct/P04DataCheckInfoExample/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataOmitDoc(dataMap, initFilePath, outFilePath)


