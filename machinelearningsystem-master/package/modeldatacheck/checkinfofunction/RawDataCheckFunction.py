from package.modeldatacheck.checkinfofunction.CommonCheckFunction import CommonCheckFunction


class RawDataCheckFunction(CommonCheckFunction) :

    def RawDataCheck_isnotnull(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnull(columnInfo)
        return mapColumn, mainColumn

    def RawDataCheck_isnotnullper(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnullper(columnInfo)
        return mapColumn , mainColumn

    def RawDataCheck_percentile(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_percentile(columnInfo)
        return mapColumn, mainColumn

    def RawDataCheck_avg(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_avg(columnInfo)
        return mapColumn, mainColumn

    def RawDataCheck_round(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_round(columnInfo)
        return mapColumn, mainColumn


