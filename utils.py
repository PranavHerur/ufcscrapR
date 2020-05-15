import urllib.request
import re
import pandas as pd
import os.path
from datetime import datetime


def get_url(url):
	return urllib.request.urlopen(urllib.request.Request(url)).read().decode('utf-8')


# maintain order found, eliminate duplicates
def unique_urls(urls):
	result = []
	for m in urls:
		if m not in result:
			result.append(m)
	return result


def get_event_urls(html):
	p = re.compile('<a href="(http://www\.ufcstats\.com/event-details/\w+)"')
	return [m.strip() for m in p.findall(html)]


def get_max_crawled_date():
	if os.path.isfile("ufcscrapR-data/fight_list.csv"):
		df = pd.read_csv("ufcscrapR-data/fight_list.csv")
		df.date = pd.to_datetime(df.date)
		return df.date.max()
	else:
		return datetime.strptime("November 12, 1993", "%B %d, %Y")


def get_max_fight_id():
	if os.path.isfile("ufcscrapR-data/fight_list.csv"):
		df = pd.read_csv("ufcscrapR-data/fight_list.csv")
		df.date = pd.to_datetime(df.date)
		return df.fight_id.max()
	else:
		return 0

