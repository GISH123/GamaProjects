import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import copy

# 該程式主要為基本執行模型的檔案

if __name__ == "__main__":

    basicMakeInfo = {
        "runtype": ["runmodel"]
        , "builduser": ["vicying"]
        , "makedate": ["2022-08-01"]
        , "productname": ["ExampleProduct"]
        , "project": ["P99Test"]
    }

    textMakeInfo = copy.deepcopy(basicMakeInfo)
    textMakeInfo["modelversion"] = ["V0_1902_0"]
    textMakeInfo["runstep"] = ["rawdata"]
    # makeModeling.main(textMakeInfo)

    runDataMakeInfo = copy.deepcopy(basicMakeInfo)
    runDataMakeInfo["modelversion"] = ["V0_1902_1"]
    runDataMakeInfo["runstep"] = ["usemodel"]
    makeModeling.main(runDataMakeInfo)

    #runDataMakeInfo["runstep"] = ["rawdata", "preprocess", "usemodel"]