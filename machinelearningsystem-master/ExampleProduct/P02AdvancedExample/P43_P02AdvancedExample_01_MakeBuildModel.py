import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["vicying"]
        , "makedate": ["2022-01-01"]
        , "productname": ["ExampleProduct"]
        , "project": ["P02AdvancedExample"]
        , "rawdataversion": ["R1_0_2"]
        , "runstep": ["rawdata"]
    })




