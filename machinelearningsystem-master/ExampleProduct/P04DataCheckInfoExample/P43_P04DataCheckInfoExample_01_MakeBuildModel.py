import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 請參考 P02AdvancedExample

if __name__ == "__main__":

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["vicying"]
        , "makedate": ["2022-01-01"]
        , "productname": ["ExampleProduct"]
        , "project": ["P04DataCheckInfoExample"]
        , "rawdataversion": ["R1_0_3"]
        , "preprocessversion": ["P1_0_3"]
        , "usemodelversion": ["M1_0_3"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })




