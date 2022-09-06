import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-18"]
        , "productname": ["maple"]
        , "project": ["ExternalDataManage"]
        , "rawdataversion": ["R1_0_1"]
        , "preprocessversion": ["P1_0_1"]
        , "usemodelversion": ["M1_0_1"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
        , "modelversion": ["V1_0_1"]
    })

