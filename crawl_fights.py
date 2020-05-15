import pandas as pd
from bs4 import BeautifulSoup

from .utils import get_url, get_max_crawled_date

per_round_cols = [
		'Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td', 'Td %', 'Sub. att', 'Pass', 'Rev.']
totals_cols = [
	'Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td', 'Td %', 'Sub. att', 'Pass', 'Rev.',
	'Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']
signif_add_on_cols = ['Fighter', 'Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']
def crawl_fights(df):
	totals_rows, per_round_rows, fight_details = [], [], []
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
		# update & order columns for fight_list
		fight_details_cols = [
			"fight_id", "fight_name", "round", "time", "time_format", "referee", "title_fight",
			"fight_of_night", "performance_bonus"
		]
		df1 = pd.DataFrame(columns=fight_details_cols, data=fight_details)
		df = df[["fight_id"] + list(set(list(df)).difference(df1))]
		ordered_cols = [
			"fight_id","event_id","date","weight_class","winner","loser","win_by","method","round","time",
			"time_format","title_fight","fight_of_night","performance_bonus","fight_name","referee","attendance",
			"location","event_name","url"]
		pd.concat([df1.merge(df, how="inner", on="fight_id")[ordered_cols], pd.read_csv("ufcscrapR-data/fight_list.csv")]) \
			.to_csv("ufcscrapR-data/fight_list.csv", index=False)

		# order fight_stats columns for writing to disk
		df = clean_fight_df(pd.DataFrame(columns=totals_cols + ["fight_id"], data=totals_rows))
		ordered_cols = [
			"fight_id", "fighter", "kd", "sig_str_landed", "sig_str_attempted", "signif_str_rate",
			"total_strikes_landed",
			"total_strikes_attempted", "sub_att", "pass", "rev", "takedown_landed", "takedown_attempted", "td_rate",
			"head_landed", "head_attempted", "body_landed", "body_attempted", "leg_landed", "leg_attempted",
			"distance_landed",
			"distance_attempted", "clinch_landed", "clinch_attempted", "ground_landed", "ground_attempted"]
		pd.concat([df[ordered_cols], pd.read_csv("ufcscrapR-data/fight_stats.csv")]) \
			.to_csv("ufcscrapR-data/fight_stats.csv", index=False)

		# order rbr columns for writing to disk
		df = clean_fight_df(pd.DataFrame(columns=per_round_cols + signif_add_on_cols[1:] + ["round", "fight_id"], data=per_round_rows))
		ordered_cols = [
			"fight_id", "fighter", "round", "kd", "sig_str_landed", "sig_str_attempted", "signif_str_rate",
			"total_strikes_landed",
			"total_strikes_attempted", "sub_att", "pass", "rev", "takedown_landed", "takedown_attempted", "td_rate",
			"head_landed", "head_attempted", "body_landed", "body_attempted", "leg_landed", "leg_attempted",
			"distance_landed",
			"distance_attempted", "clinch_landed", "clinch_attempted", "ground_landed", "ground_attempted"]
		pd.concat([df[ordered_cols], pd.read_csv("ufcscrapR-data/rbr.csv")]) \
			.to_csv("ufcscrapR-data/rbr.csv", index=False)


def clean_fight_df(df):
	df.rename(columns={"Sig. str.": "sig_str","Total str.": "total_strikes", "Td": "takedown"}, inplace=True)
	cols = ["sig_str", "total_strikes", "takedown", "Head", "Body", "Leg", "Distance", "Clinch", "Ground"]

	clean_of_col = lambda o: o.split(" of ")
	for col in cols:
		df[col.casefold()+"_landed"] = df.apply(lambda r: clean_of_col(r[col])[0], axis=1)
		df[col.casefold()+"_attempted"] = df.apply(lambda r: clean_of_col(r[col])[1], axis=1)

	for col in ["Sig. str. %", "Td %"]:
		df[col] = df.apply(lambda r: int(r[col][:-1])/100, axis=1)
	df.rename(columns={"Sig. str. %": "signif_str_rate","Td %": "td_rate","Sub. att":"sub_att","Rev.": "rev"}, inplace=True)
	df.drop(columns=cols, inplace=True)
	df.rename(columns={c: c.casefold() for c in list(df)}, inplace=True)


	print(df.head().to_string())
	return df


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
