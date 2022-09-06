from package.modeldatacheck.checkinfofunction.CommonCheckFunction import CommonCheckFunction


class PreProcessCheckFunction(CommonCheckFunction) :

    def PreProcessCheck_isnotnull(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnull(columnInfo)
        return mapColumn , mainColumn

    def PreProcessCheck_isnotnullper(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnullper(columnInfo)
        return mapColumn , mainColumn

    def PreProcessCheck_percentile(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_percentile(columnInfo)
        return mapColumn , mainColumn

    def PreProcessCheck_avg(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_avg(columnInfo)
        return mapColumn , mainColumn

    def PreProcessCheck_round(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_round(columnInfo)
        return mapColumn , mainColumn
