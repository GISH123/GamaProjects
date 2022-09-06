import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-18"]
        , "productname": ["maple"]
        , "project": ["PredictY"]
        , "modelversion": ["V0_9001_0"]
        , "rawdataversion": ["R0_9001_0"]
        , "runstep": ["rawdata"]
    })
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-18"]
        , "productname": ["maple"]
        , "project": ["PredictY"]
        , "modelversion": ["V0_9001_1"]
        , "rawdataversion": ["R0_9001_1"]
        , "preprocessversion": ["P0_9001_1"]
        , "usemodelversion": ["M0_9001_99"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })
