import re
import time
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from .utils import get_url, unique_urls, get_event_urls, get_max_crawled_date


def crawl_fighters():
	p = re.compile('<a href="(http://www.ufcstats.com/fighter-details/\w+)"')
	html = get_url("http://www.ufcstats.com/statistics/fighters?char=a&page=all")
	df: pd.DataFrame = pd.read_html(html)[0].drop("Belt", axis=1).drop(index=0)

	# maintain order for duplicate links
	df["url"] = unique_urls(p.findall(html))

	print(df.head().to_string())
	print(df.shape)
	df.to_csv("ufcscrapR-data/fighters.csv", index=False)


def crawl_event_list():
	html = get_url("http://www.ufcstats.com/statistics/events/completed?page=all")

	df = pd.read_html(html)[0].dropna()

	names, dates = [], []
	for _, row in df.iterrows():
		name, date = row["Name/date"].split("  ")
		names.append(name)
		dates.append(date)

	df["event_id"] = [i for i in range(df.shape[0], 0, -1)]
	df["name"] = names
	df["date"] = dates
	df["location"] = df["Location"]
	df["url"] = get_event_urls(html)

	print(df.head().to_string())
	df.set_index("event_id", inplace=True)
	df[["name", "date", "location", "url"]].to_csv("ufcscrapR-data/events.csv")


def crawl_events():
	p = re.compile('<a class="b-flag b-flag_style_\w+" href="(http://www.ufcstats.com/fight-details/\w+)"')

	fight_list = pd.DataFrame()

	event_df = pd.read_csv("ufcscrapR-data/events.csv")
	event_df.date = pd.to_datetime(event_df.date)
	event_df.query("'{0}' < date < '{1}'".format(get_max_crawled_date(), datetime.now().date()), inplace=True)

	for _, row in event_df.iterrows():
		event_id = row.event_id
		url = row.url

		print(url)
		html = get_url(url)

		fight_df: pd.DataFrame = pd.read_html(html)[0]

		# get fight meta data
		# data, location, attendance
		soup = BeautifulSoup(html, 'html.parser')
		for i, tag in enumerate(soup.find_all('li', class_="b-list__box-list-item")):
			a = tag.find('i').text.strip()
			fight_df[a[:-1]] = [tag.text.strip().replace(a, "").strip() for _ in range(fight_df.shape[0])]

		fight_df["event_id"] = [event_id for _ in range(fight_df.shape[0])]
		fight_df["event_name"] = [soup.find("span", class_="b-content__title-highlight").text.strip() for _ in range(fight_df.shape[0])]

		# get fight url
		fight_df["url"] = unique_urls(p.findall(html))

		# append to crawled
		fight_list = pd.concat([fight_list, fight_df])
		time.sleep(5)

	if fight_list.shape[0] > 0:
		# assign fight_id
		fight_list["fight_id"] = [i for i in range(fight_list.shape[0], 0, -1)]
		fight_list.rename(columns={"Weight class": "weight_class", "Method": "method"}, inplace=True)
		fight_list["win_by"] = fight_list.apply(lambda r: r.method.split("  ")[0], axis=1)
		fight_list["method"] = fight_list.apply(lambda r: clean_method(r), axis=1)

		build_fights_from_raw(fight_list)


def clean_method(row):
	method = row.method.split("  ")
	return method[1] if len(method) > 1 else ""


def build_fights_from_raw(raw_df):
	# split columns
	dupe_cols = list(raw_df)[8:-1]
	split_cols = list(raw_df)[1:6]
	split_rows = []
	for i, row in raw_df.iterrows():
		# some columns get read as a single row
		winner, loser = [], []
		for col in split_cols:
			fw, fl = row[col].split("  ")
			winner.append(fw)
			loser.append(fl)

		split_rows.append(["W", *winner, row.weight_class, row.win_by, row.method, *row[dupe_cols].values])
		split_rows.append(["L", *loser, row.weight_class, row.win_by, row.method, *row[dupe_cols].values])

	# write 1 fighter per line
	fights = pd.DataFrame(columns=["W", *split_cols, "weight_class", "win_by", *list(raw_df)[7:-1]], data=split_rows)
	fights.rename(columns={c: c.casefold() for c in list(fights)}, inplace=True)
	ordered_cols = ['event_id', 'fight_id', 'fighter', "w", 'str', 'td', 'sub', 'pass', 'weight_class', 'win_by', 'method',
					'round', 'time', 'date', 'location', 'attendance', 'event_name', 'url']
	print(fights[ordered_cols].head().to_string())
	fights[ordered_cols].to_csv("ufcscrapR-data/fights_per_fighter.csv", index=False)

	# write to disk
	# 1 fight per line
	fight_list = pd.DataFrame(columns=["winner", "loser", "weight_class", "win_by", *list(raw_df)[7:-1]],
							  data=([*row.Fighter.split("  "), row.weight_class, row.win_by, row.method,
									 *row[list(raw_df)[8:-1]].values] for _, row in raw_df.iterrows())
							  )
	fight_list.rename(columns={c: c.casefold() for c in list(fight_list)}, inplace=True)
	ordered_cols = ['event_id', 'fight_id', 'winner', 'loser', 'weight_class', 'win_by', 'method', 'round', 'time',
					'date', 'location', 'attendance', 'event_name', 'url']
	print(fight_list[ordered_cols].head().to_string())
	fight_list[ordered_cols].to_csv("ufcscrapR-data/fight_list.csv", index=False)


