from package.modeldatacheck.checkinfofunction.CommonCheckFunction import CommonCheckFunction


class ModelResultCheckFunction(CommonCheckFunction) :

    def ModelResultCheck_isnotnull(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnull(columnInfo)
        return mapColumn, mainColumn

    def ModelResultCheck_isnotnullper(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnullper(columnInfo)
        return mapColumn , mainColumn

    def ModelResultCheck_percentile(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_percentile(columnInfo)
        return mapColumn, mainColumn

    def ModelResultCheck_avg(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_avg(columnInfo)
        return mapColumn, mainColumn

    def ModelResultCheck_round(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_round(columnInfo)
        return mapColumn, mainColumn


