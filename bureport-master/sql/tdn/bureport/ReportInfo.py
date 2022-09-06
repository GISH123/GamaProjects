class ReportInfo() :
    def __init__(self):
        self.worldNameArr = [
            "01"
        ]
        self.tableNumberArr = [
            "1001", "1002", "1003"
            , "1101", "1102", "1103", "1804", "1806"
            , "1131", "1132", "1133", "1134", "1135", "1136", "1137"
            , "6002", "6011", "6012", "6019", "6509", "6609"
        ]
        self.layerArr = [
            # Login + ----------------------------------------------------------------------------------------------------
            "101"
            # All Data ----------------------------------------------------------------------------------------------------
            , "111"
            # Tag Data ----------------------------------------------------------------------------------------------------
        ]
        self.layerInfoArrMap = {
            # Login + ----------------------------------------------------------------------------------------------------
            "101": ["1001", "1002", "1003", "1101", "1102", "1103", "1804", "1806"
                    , "1131", "1132", "1133", "1134", "1135", "1136", "1137"
                    , "6002", "6011", "6012", "6019"]
            # All Data ----------------------------------------------------------------------------------------------------
            , "111": ["6509", "6609"]
        }

        self.tableNumberInfoMap = {
            "1002": {"interDependence": ["1804", "1806"]}
            , "1103": {"interDependence": ["1131", "1132", "1133"]}
            , "1131": {"interDependence": ["1134", "1135", "1136", "1137"]}
            , "6019": {"interDependence": ["6011", "6012"]}
        }