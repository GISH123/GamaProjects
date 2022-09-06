import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None

import common.P43_01_MakeModelingCommon as makeModeling

if __name__ == "__main__":

    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["LoginTag"]
    #     , "rawdataversion": ["R0_1_1"]
    #     , "preprocessversion": ["P0_1_1"]
    #     , "usemodelversion": ["M0_1_1"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["vicying"]
        , "makedate": ["2022-01-01"]
        , "productname": ["maple"]
        , "project": ["LoginTag"]
        , "usemodelversion": ["M0_1_3"]
        , "runstep": ["usemodel"]
    })
