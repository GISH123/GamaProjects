import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要是建立相關資料，詳細說明請參考 P02AdvancedExample

if __name__ == "__main__":

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["vicying"]
        , "makedate": ["2022-01-01"]
        , "productname": ["ExampleProduct"]
        , "project": ["P03DocumentExample"]
        , "rawdataversion": ["R1_0_3"]
        , "preprocessversion": ["P1_0_3"]
        , "usemodelversion": ["M1_0_3"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })




