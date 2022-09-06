import os , sys, datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None

from package.common.inputCtrl import inputCtrl
import maple.P00_MakeLogin as P00Ctrl
import maple.P10_MakePartition as P10Ctrl
import maple.P20_MakeExtract as P20Ctrl
import maple.P30_MakeMiddle as P30Ctrl
import maple.P50_MakeALLMonthFirst as P50Ctrl
import maple.P70_UseModel as P70Ctrl


def main(run_parametersData = {}):

    if run_parametersData == {}:
        run_parametersData = inputCtrl.makeParametersData(sys.argv)

    ################################################################################################
    runP00 = False
    runP10 = False
    runP20 = False
    runP30 = True
    runP50 = False
    runP70 = True

    ####################################################################################################
    """
        runP00
    """
    if runP00 == True :
        P00Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP10
    """
    if runP10 == True:
        P10Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP20
    """
    if runP20 == True:
        P20Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP30
    """
    if runP30 == True:
        P30Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP50
    """
    if runP50 == True:
        P50Ctrl.main()
    ####################################################################################################
    """
        runP70
    """
    if runP70 == True:
        P70Ctrl.main(run_parametersData)

if __name__ == "__main__" :
    nowTime = datetime.datetime.now()
    main()