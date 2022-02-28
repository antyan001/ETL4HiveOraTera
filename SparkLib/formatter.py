class col:
    HEADER = '\033[95m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'
    RED = '\033[31m'


class colprint:
    def __init__(self, text, Error=False):
        if Error:
            line='{y}'
        else:
            line = '{g}'
        print((line+text+'{end}').format(h=col.HEADER, g=col.GREEN, end=col.ENDC, y=col.YELLOW, red=col.RED))

from datetime import datetime
from dateutil.relativedelta import relativedelta

class datehelper:

    def monthlist_fast(dates, form='%Y_%m'):
        start, end = dates
        total_months = lambda dt: dt.month + 12 * dt.year
        mlist=[]
        for tot_m in range(total_months(start)-1, total_months(end)):
            y, m = divmod(tot_m, 12)
            mlist.append(datetime(y, m+1, 1).strftime(form))
        return mlist

    def last_n_months(date, n):
        return datehelper.monthlist_fast([date + relativedelta(months=-n), date + relativedelta(months=-1)])
