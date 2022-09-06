import datetime as dt


def getMonthList(months, startDate, returnLastDay=True):

    if not returnLastDay:
        return getMonthList(months, startDate)[0]

    dateList = []
    LastDayList = []
    for i in range(months):
        dateList.append(startDate)
        startDate = startDate + dt.timedelta(days=31)
        startDate = startDate.replace(day=1)
        LastDayList.append(startDate - dt.timedelta(days=1))

    return dateList, LastDayList


def getContinueDayList(startDate, endDate, gap=1, returnLastDay=False):

    if not returnLastDay:
        return getContinueDayList(startDate, endDate, gap=1, returnLastDay=True)[0]

    dateList = []
    LastDayList = []
    while startDate <= endDate:
        dateList.append(startDate)
        startDate = startDate + dt.timedelta(days=1 * gap)
        LastDayList.append(startDate)
    return dateList, LastDayList


def getPreviousWeekday(date, weekday=3):
    if date.isoweekday() == weekday:
        date -= dt.timedelta(days=7)
    while date.isoweekday() != weekday:
        date -= dt.timedelta(days=1)
    return date
