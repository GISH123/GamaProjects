import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.modeldatacheck.ModelDataCheckCtrl import ModelDataCheckCtrl
from package.modeling.ModelingCtrl import ModelingCtrl
from package.common.inputCtrl import inputCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.modelmanagement.entity.ModelVersionEntity import ModelVersionEntity
from package.modelmanagement.entity.ModelRecordEntity import ModelRecordEntity
from package.modelmanagement.ModelException import ModelException
from MLOPSConfig import MLOPSConfig
from dotenv import load_dotenv
import datetime
import json
import re
import pandas
import copy
import time
import json

modelingCtrl = ModelingCtrl()
modelDataCheckCtrl = ModelDataCheckCtrl()
modelVersionEntityCtrl = ModelVersionEntity()
modelRecordEntityCtrl = ModelRecordEntity()
mlopsConfig = MLOPSConfig()


def main(parametersData = {}):
    runType = ""
    buildUser = ""
    makeDateStrArr = []
    productName = ""
    project = ""
    modelVersionArr = []
    rawdataVersionArr = []
    preprocessVersionArr = [] # V2_0_0
    usemodelVersionArr = [] # V2_0_0
    runStepArr = []  # rawdata preprocess usemodel
    parameter = {}

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "runtype":
            runType = parametersData[key][0]
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "builduser":
            buildUser = parametersData[key][0]
        if key == "productname":
            productName = parametersData[key][0]
        if key == "project":
            project = parametersData[key][0]
        if key == "modelversion":
            modelVersionArr = parametersData[key]
        if key == "rawdataversion":
            rawdataVersionArr = parametersData[key]
        if key == "preprocessversion":
            preprocessVersionArr = parametersData[key]
        if key == "usemodelversion":
            usemodelVersionArr = parametersData[key]
        if key == "runstep":
            runStepArr = parametersData[key]
        if key == "parameter":
            parameter = parametersData[key]

    runType = "" if runType == "" else runType
    buildUser = "" if buildUser == "" else buildUser
    makeDateStrArr = [] if makeDateStrArr == [] else makeDateStrArr
    productName = "" if productName == [] else productName
    project = "" if project == [] else project
    modelVersionArr = [] if modelVersionArr == [] else modelVersionArr
    rawdataVersionArr = [None] if rawdataVersionArr == [] and runType == "buildmodel" else rawdataVersionArr
    preprocessVersionArr = [None] if preprocessVersionArr == [] and runType == "buildmodel" else preprocessVersionArr
    usemodelVersionArr = [None] if usemodelVersionArr == [] and runType == "buildmodel" else usemodelVersionArr
    runStepArr = [] if runStepArr == [] else runStepArr
    parameter = {} if parameter == {} else parameter

    # 輸入檢查
    checkInput(runType,buildUser,makeDateStrArr,productName,project,modelVersionArr,rawdataVersionArr,preprocessVersionArr,usemodelVersionArr,runStepArr)

    makeTimeArr = list(map(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"), makeDateStrArr))  # map lambda X -> 將X的值做相關運算並且回傳

    modelInfoArr = []
    if runType == "buildmodel" :
        modelInfoArr = makeBuildModelInfo(runType, makeTimeArr, buildUser, productName, project,modelVersionArr, rawdataVersionArr, preprocessVersionArr, usemodelVersionArr, runStepArr,parameter)
    elif runType == "runmodel" :
        modelInfoArr = makeRunModelInfo(runType, makeTimeArr, buildUser, productName, project, modelVersionArr, runStepArr,parameter)
    elif runType == "checkdata" :
        modelInfoArr = makeCheckInfo(runType, makeTimeArr, buildUser, productName, project,modelVersionArr, rawdataVersionArr, preprocessVersionArr, usemodelVersionArr, runStepArr)

    for modelInfo in modelInfoArr :
        try :
            if runType == "buildmodel" or runType == "runmodel" :
                if runType == "buildmodel" :
                    modelVersionEntityCtrl.setEntity(modelVersionEntityCtrl.makeModelVersionEntityByModelInfo(modelInfo))
                    modelVersionEntityCtrl.deleteOldModelVersionByModelInfo(modelInfo)
                    modelVersionEntityCtrl.insertEntity()
                    print("finish build model , version is {} , fullcode is {}".format( modelInfo["modelVersion"],modelInfo["modelVersionFullCode"])) # buildmodel 只部屬不跑
                elif runType == "runmodel" :
                    modelRecordEntityCtrl.setEntity(modelRecordEntityCtrl.makeModelRecordEntityByModelInfo(modelInfo))
                    modelRecordEntityCtrl.setColumnValue("state" , "RUN")
                    modelRecordEntityCtrl.insertEntity()
                    modelingCtrl.makeModeling(modelInfo)
                    modelRecordEntityCtrl.setColumnValue("state" , "FINISH")
                    time.sleep(5)
                    modelRecordEntityCtrl.setColumnValue("resultjson" , json.dumps(modelInfo["result"],ensure_ascii=False)  if "result" in modelInfo.keys() else '{}')
                    modelRecordEntityCtrl.updateEntity()
            elif runType == "checkdata" :
                modelDataCheckCtrl.checkModelData(modelInfo)
                modelDataCheckCtrl.alarmModelData(modelInfo)
        except Exception as e:
            print(e.message, e.args)

def checkInput(runType,buildUser,makeDateStrArr,productName,project,modelVersionArr,rawdataVersionArr,preprocessVersionArr,usemodelVersionArr,runStepArr):

    if runType not in ["buildmodel","runmodel","checkdata"] :
        raise ModelException("runtype is error , runtype value is {} ? ".format(runType))

    if runType in ["buildmodel"] and len(modelVersionArr) >= 2:
        raise ModelException("runtype is buildmodel , but modelversion's len >= 2 ?")

    if runType == "runmodel" and len(rawdataVersionArr) > 0:
        raise ModelException("runtype is runmodel , but rawdataversion's len > 0 ?")

    if runType == "runmodel" and len(preprocessVersionArr) > 0:
        raise ModelException("runtype is runmodel , but preprocessversion's len > 0 ?")

    if runType == "runmodel" and len(usemodelVersionArr) > 0:
        raise ModelException("runtype is runmodel , but usemodelversion's len > 0 ?")

    if runType in ["buildmodel","runmodel"] and buildUser not in mlopsConfig.buildUsers :
        raise ModelException("builduser is error , builduser value is {} ?".format(buildUser))

    if productName not in mlopsConfig.productNames :
        raise ModelException("productname is error , productname value is {} ?".format(productName))

    if project == None or project == "" :
        raise ModelException("project is None ?".format(project))

    for makeDateStr in makeDateStrArr :
        try :
            datetime.datetime.strptime(makeDateStr, "%Y-%m-%d")
        except :
            raise ModelException("makedate is error , makedate value is {} ?".format(makeDateStr))

    if "rawdata" in runStepArr :
        for rawdataVersion in rawdataVersionArr :
            if rawdataVersion == None :
                continue
            if rawdataVersion[0] not in ["\\","R",'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'] :
                raise ModelException("rawdataversion is error , rawdataversion value is {} ?".format(rawdataVersion))
            elif rawdataVersion[0] in ["R"] :
                if re.match("[R](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", rawdataVersion) == None \
                        or re.match("[R](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", rawdataVersion).group() != rawdataVersion :
                    raise ModelException("rawdataversion is error , rawdataversion value is {} ?".format(rawdataVersion))

    if "preprocess" in runStepArr:
        for preprocessVersion in preprocessVersionArr :
            if preprocessVersion == None :
                continue
            if preprocessVersion[0] not in ["\\","P",'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'] :
                raise ModelException("preprocessversion is error , preprocessversion value is {} ?".format(preprocessVersion))
            elif preprocessVersion[0] in ["P"] :
                if re.match("[P](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", preprocessVersion) == None \
                        or re.match("[P](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", preprocessVersion).group() != preprocessVersion :
                    raise ModelException("preprocessversion is error , preprocessversion value is {} ?".format(preprocessVersion))

    if "usemodel" in runStepArr:
        for usemodelVersion in usemodelVersionArr :
            if usemodelVersion == None :
                continue
            if re.match("[M](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", usemodelVersion) == None \
                    or re.match("[M](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", usemodelVersion).group() != usemodelVersion:
                raise ModelException("usemodelversion is error , usemodelversion value is {} ?".format(usemodelVersion))

    for modelVersion in modelVersionArr :
        if re.match("[V](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", modelVersion) == None \
                or re.match("[V](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])[_](([1-9][0-9]*)|[0-9])", modelVersion).group() != modelVersion:
            raise ModelException("modelversion is error , modelversion value is {} ?".format(modelVersion))

# 製作相關版本號
def makeBuildModelInfo (runType,makeTimeArr,buildUser,productName,project,modelVersionArr,rawdataVersionArr,preprocessVersionArr,usemodelVersionArr,runStepArr,parameter) :
    modelInfoArr = []
    if len(modelVersionArr) >= 1 :
        lastModelVersion = modelVersionArr[0]
        last = int(lastModelVersion.split("_")[2])
    else :
        lastModelVersion = modelVersionEntityCtrl.getLastModelVersionNo(productName, project)
        last = int(lastModelVersion.split("_")[2])
        last = last + 1

    for makeTime in makeTimeArr:
        for usemodelVersionStr in usemodelVersionArr:
            for preprocessVersionStr in preprocessVersionArr:
                for rawdataVersionStr in rawdataVersionArr:
                    modelInfo = {
                        "runType": runType
                        , "buildUser": buildUser
                        , "productName": productName
                        , "project": project
                        , "modelVersion": lastModelVersion.split("_")[0] + '_' + lastModelVersion.split("_")[1] + '_' + str(last)
                        , "modelVersionFullCode": str(rawdataVersionStr) + '-' + str(preprocessVersionStr) + '-' + str(usemodelVersionStr)
                        , "usemodelVersion": usemodelVersionStr
                        , "preprocessVersion": preprocessVersionStr
                        , "rawdataVersion": rawdataVersionStr
                        , "makeTime": makeTime.strftime("%Y-%m-%d")
                        , "runStepArr": runStepArr
                        , "parameter":parameter
                    }
                    modelInfoArr.append(modelInfo)
                    last = last + 1
    return modelInfoArr

def makeRunModelInfo (runType,makeTimeArr,buildUser,productName,project,modelVersionArr,runStepArr,parameter) :
    modelInfoArr = []
    for makeTime in makeTimeArr:
        for modelVersionStr in modelVersionArr:
            modelVersionEntity = modelVersionEntityCtrl.getModelVersionByProductNameProjectModelVersion(productName,project,modelVersionStr)
            modelInfo = {
                "modelversionid" : modelVersionEntity["modelversionid"]
                , "modelrecordid" : modelRecordEntityCtrl.getNextPrimaryKeyId()
                , "runType": runType
                , "buildUser": buildUser
                , "productName": productName
                , "project": project
                , "modelVersion": modelVersionEntity["modelversion"]
                , "modelVersionFullCode": modelVersionEntity["modelversionfullcode"]
                , "usemodelVersion": modelVersionEntity["usemodelversion"]
                , "preprocessVersion": modelVersionEntity["preprocessversion"]
                , "rawdataVersion": modelVersionEntity["rawdataversion"]
                , "makeTime": makeTime.strftime("%Y-%m-%d")
                , "runStepArr": modelVersionEntity["runstep"].split(',') if runStepArr == [] else runStepArr
                , "parameter": json.loads(modelVersionEntity["parameterjson"]) if parameter == {} and modelVersionEntity["parameterjson"] != None else parameter
            }
            modelInfoArr.append(modelInfo)
    return modelInfoArr

def makeCheckInfo (runType,makeTimeArr,buildUser,productName,project,modelVersionArr,rawdataVersionArr,preprocessVersionArr,usemodelVersionArr,runStepArr) :
    modelInfoArr = []

    modelInitInfo = {
        "runType": runType
        , "productName": productName
        , "project": project
    }
    stepInfoArr = [
        ["ModelResult",modelVersionArr]
        , ["ModelScore",modelVersionArr]
        , ["RawData",rawdataVersionArr]
        , ["PreProcess",preprocessVersionArr]
        , ["UseModel",usemodelVersionArr]
    ]

    for makeTime in makeTimeArr:
        for stepInfo in stepInfoArr :
            step = stepInfo[0]
            versionArr = stepInfo[1]
            for versionStr in versionArr:
                modelInfo = copy.deepcopy(modelInitInfo)
                modelInfo["version"] = versionStr
                modelInfo["step"] = step
                modelInfo["makeTime"] = makeTime.strftime("%Y-%m-%d")
                modelInfoArr.append(modelInfo)

    return modelInfoArr




