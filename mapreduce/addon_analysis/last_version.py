import urllib2
import simplejson as json
from datetime import datetime, date

file = urllib2.urlopen("https://latte.ca/cgi-bin/status.cgi")
content = json.load(file)
last = filter(lambda x: datetime.today() >= datetime.strptime(x['sDate'], "%Y-%m-%d"), content)[-1]
print last['data']['release'].split(' ')[-1]
