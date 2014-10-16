import urllib2
import simplejson as json
import sys
from datetime import datetime, date

day = datetime.strptime(sys.argv[1], "%Y%m%d")
file = urllib2.urlopen("https://latte.ca/cgi-bin/status.cgi")
content = json.load(file)
last = filter(lambda x: day >= datetime.strptime(x['sDate'], "%Y-%m-%d"), content)[-1]
print last['data']['release'].split(' ')[-1]
