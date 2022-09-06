
class CommonAlarmFunction():

    def CommonAlarm_FixedValueComparison(self,alarmInfo):
        comparison = alarmInfo['comparison']
        value = alarmInfo['value']
        level = alarmInfo['level']
        caseWhenThenCondition = "WHEN AA.checknumbervalue {} {} THEN '{}' ".format(comparison, value, level)
        caseWhenThenMassage = "WHEN AA.checknumbervalue {} {} THEN 'value {} {} , level is {}' ".format(comparison, value,comparison, value, level)
        return caseWhenThenCondition , caseWhenThenMassage



