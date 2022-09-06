import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import copy
# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":
    basicMakeInfo = {
        "runtype": ["buildmodel"]
        , "builduser": ["vicying"]
        , "makedate": ["2022-08-01"]
        , "productname": ["ExampleProduct"]
        , "project": ["P99Test"]
    }

    textMakeInfo = copy.deepcopy(basicMakeInfo)
    textMakeInfo["modelversion"] = ["V0_1902_0"]
    textMakeInfo["rawdataversion"] = ["R0_1902_0"]
    textMakeInfo["runstep"] = ["rawdata"]
    #makeModeling.main(textMakeInfo)

    runDataMakeInfo = copy.deepcopy(basicMakeInfo)
    runDataMakeInfo["modelversion"] = ["V0_1902_1"]
    runDataMakeInfo["rawdataversion"] = ["R0_1902_1"]
    runDataMakeInfo["preprocessversion"] = ["P0_1902_1"]
    runDataMakeInfo["usemodelversion"] = ["M0_1902_1"]
    runDataMakeInfo["runstep"] = ["rawdata", "preprocess", "usemodel"]
    makeModeling.main(runDataMakeInfo)

    #
    # runDataMakeInfo = copy.deepcopy(basicMakeInfo)
    # runDataMakeInfo["modelversion"] = ["V0_1902_2"]
    # runDataMakeInfo["preprocessversion"] = ["P0_1902_2"]
    # runDataMakeInfo["runstep"] = ["preprocess", "usemodel"]
    # makeModeling.main(runDataMakeInfo)


