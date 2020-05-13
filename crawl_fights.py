import pandas as pd
from bs4 import BeautifulSoup

from .utils import get_url, get_max_crawled_date

per_round_cols = [
		'Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td', 'Td %', 'Sub. att', 'Pass', 'Rev.']
totals_cols = [
	'Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td', 'Td %', 'Sub. att', 'Pass', 'Rev.',
	'Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']
signif_add_on_cols = ['Fighter', 'Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']
def crawl_fights():
	totals_rows, per_round_rows, fight_details = [], [], []
	df = pd.read_csv("ufcscrapR-data/fight_list.csv")
	df.date = pd.to_datetime(df.date)

	for _, crawl_row in df.query("'{0}' < date".format(get_max_crawled_date())).iterrows():
		print(crawl_row.fight_id)
		try:
			html = get_url(crawl_row.url)
			general_total, gen_per_round, signif_total, signif_per_round = pd.read_html(html)

			# get extra fight details
			fight_details.append(build_fight_details_row(crawl_row.fight_id, html))

			# build totals
			totals_rows.extend(split_combined_rows(general_total, signif_total[signif_add_on_cols], crawl_row))

			# build per round
			per_round_rows.extend(create_per_round(gen_per_round, signif_per_round, crawl_row))
		except Exception as e:
			print(e)

	if totals_rows:
		fight_details_cols = [
			"fight_id", "fight_name", "round", "time", "time_format", "referee", "title_fight",
			"fight_of_night", "performance_bonus"
		]
		df1 = pd.DataFrame(columns=fight_details_cols, data=fight_details)
		df2 = pd.read_csv("ufcscrapR-data/fight_list.csv")
		df2 = df2[["fight_id"] + list(set(list(df2)).difference(df1))]
		df1.merge(df2, how="inner", on="fight_id").to_csv("ufcscrapR-data/fight_list.csv", index=False)

		totals_df = pd.DataFrame(columns=totals_cols + ["fight_id"], data=totals_rows)
		clean_fight_df(totals_df, "fight_stats.csv")

		per_round_df = pd.DataFrame(columns=per_round_cols + signif_add_on_cols[1:] + ["round", "fight_id"],
									data=per_round_rows)
		clean_fight_df(per_round_df, "rbr.csv")


def clean_fight_df(df, fname):
	df.rename(columns={"Sig. str.": "sig_str","Total str.": "total_strikes", "Td": "takedown"}, inplace=True)
	cols = ["sig_str", "total_strikes", "takedown", "Head", "Body", "Leg", "Distance", "Clinch", "Ground"]

	clean_of_col = lambda o: o.split(" of ")
	for col in cols:
		df[col.casefold()+"_landed"] = df.apply(lambda r: clean_of_col(r[col])[0], axis=1)
		df[col.casefold()+"_attempted"] = df.apply(lambda r: clean_of_col(r[col])[1], axis=1)

	for col in ["Sig. str. %", "Td %"]:
		df[col] = df.apply(lambda r: int(r[col][:-1])/100, axis=1)

	df.drop(columns=cols, inplace=True)

	print(df.head().to_string())
	df.to_csv("ufcscrapR-data/"+fname, index=False)


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


def split_row(row, crawl_row):
	a, b = crawl_row.winner, crawl_row.loser
	if row.Fighter.startswith(crawl_row.loser):
		a = crawl_row.loser
		b = crawl_row.winner

	a, b = [a], [b]
	for val in list(row)[1:]:
		val_a, val_b = val.split("  ")
		a.append(val_a)
		b.append(val_b)

	return a, b


def split_combined_rows(gen_totals_df, signif_totals_df, crawl_row):
	a, b = split_row(gen_totals_df.iloc[0], crawl_row)
	a1, b1 = split_row(signif_totals_df.iloc[0], crawl_row)
	a.extend(a1[1:])
	b.extend(b1[1:])
	a.append(crawl_row.fight_id)
	b.append(crawl_row.fight_id)
	return [a, b]


def create_per_round(gen_per_round, signif_per_round, crawl_row):
	gen_per_round.columns = gen_per_round.columns.map('_'.join)
	gen_per_round.rename(columns={o: per_round_cols[i] for i, o in enumerate(list(gen_per_round))}, inplace=True)

	cleaned_cols = [o[0] for o in list(signif_per_round)]
	signif_per_round.columns = signif_per_round.columns.map('_'.join)
	signif_per_round.rename(columns={o: cleaned_cols[i] for i, o in enumerate(list(signif_per_round))}, inplace=True)
	signif_per_round = signif_per_round[signif_add_on_cols]

	rows = []
	# combine both per_rounds
	for i in range(gen_per_round.shape[0]):
		a, b = split_row(gen_per_round.iloc[i], crawl_row)
		a1, b1 = split_row(signif_per_round.iloc[i], crawl_row)

		# skip duplicate name column
		a.extend(a1[1:])
		b.extend(b1[1:])

		# add round
		a.append(i + 1)
		b.append(i + 1)

		# add fight_id
		a.append(crawl_row.fight_id)
		b.append(crawl_row.fight_id)

		# add to rows
		rows.append(a)
		rows.append(b)

	return rows


def build_fight_details_row(fight_id, html):
	soup = BeautifulSoup(html, 'html.parser')
	fight_name = soup.find_all('i', class_="b-fight-details__fight-title")[0].text.strip()
	fight_constants = {}
	for i, tag in enumerate(soup.find_all('i', class_="b-fight-details__text-item")[:4]):
		a = tag.find('i').text.strip()
		fight_constants[a[:-1]] = tag.text.strip().replace(a, "").strip()

	flags = {"belt": False, "fight": False, "perf": False}

	for tag in soup.find('i', 'b-fight-details__fight-title').find_all('img'):
		flag = tag["src"].split("/")[-1].split(".")[0]
		if flag in flags:
			flags[flag] = True

	return [
		fight_id, fight_name, fight_constants["Round"], fight_constants["Time"],
		fight_constants["Time format"], fight_constants["Referee"],
		flags["belt"], flags["fight"], flags["perf"]
	]
