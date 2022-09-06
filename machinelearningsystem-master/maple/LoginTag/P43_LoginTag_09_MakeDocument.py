import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from maple.LoginTag.info.RawDataInfo_LoginTag import RawDataInfo_LoginTag
from maple.LoginTag.info.PreProcessInfo_LoginTag import PreProcessInfo_LoginTag
from maple.LoginTag.info.UseModelInfo_LoginTag import UseModelInfo_LoginTag
from maple.LoginTag.info.ModelResultInfo_LoginTag import ModelResultInfo_LoginTag
from maple.LoginTag.info.ModelScoreInfo_LoginTag import ModelScoreInfo_LoginTag
from package.modeldocument.documentCtrl import DocumentCtrl

class LoginTagInfoMain(
                    RawDataInfo_LoginTag
                    , PreProcessInfo_LoginTag
                    , UseModelInfo_LoginTag
                    , ModelResultInfo_LoginTag
                    , ModelScoreInfo_LoginTag
                ):
    pass

projectInfoMain = LoginTagInfoMain()
documentCtrl = DocumentCtrl()

# 請參考 P02AdvancedExample

if __name__ == "__main__":
    dataMap = {
        "AllData" : [
            projectInfoMain.getRawDataInfo_LoginTag_R0_1_2()[1]
            , projectInfoMain.getPreProcessInfo_LoginTag_P0_1_2()[1]
            , projectInfoMain.getUseModelInfo_LoginTag_M0_1_2()[1]
            , projectInfoMain.getModelResultInfo_LoginTag_V0_0_12()[1]
            , projectInfoMain.getModelScoreInfo_LoginTag_V0_0_12()[1]
        ]
        , "ModelResult" : [
            projectInfoMain.getModelResultInfo_LoginTag_V0_0_6()[1]
            , projectInfoMain.getModelResultInfo_LoginTag_V0_0_12()[1]
        ]
        , "ModelScore" : [
            projectInfoMain.getModelScoreInfo_LoginTag_V0_0_6()[1]
            , projectInfoMain.getModelScoreInfo_LoginTag_V0_0_12()[1]
        ]
        , "UseModel" : [
            projectInfoMain.getUseModelInfo_LoginTag_M0_1_1()[1]
            , projectInfoMain.getUseModelInfo_LoginTag_M0_1_2()[1]
        ]
        , "PreProcess": [
            projectInfoMain.getPreProcessInfo_LoginTag_P0_1_1()[1]
            , projectInfoMain.getPreProcessInfo_LoginTag_P0_1_2()[1]
        ]
        , "RawData": [
            projectInfoMain.getRawDataInfo_LoginTag_R0_1_1()[1]
            , projectInfoMain.getRawDataInfo_LoginTag_R0_1_2()[1]
        ]
    }
    fileName = "maple_LoginTag_ModelUseDataDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataDocInit.xlsx'
    outFilePath = 'maple/LoginTag/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataDoc(dataMap,initFilePath,outFilePath)

    fileName = "maple_LoginTag_ModelUseDataOmitDoc.xlsx"
    initFilePath = 'common/common/file/doc/ModelUseDataOmitDocInit.xlsx'
    outFilePath = 'maple/LoginTag/file/doc/{}'.format(fileName)
    documentCtrl.MakeModelUseDataOmitDoc(dataMap, initFilePath, outFilePath)




