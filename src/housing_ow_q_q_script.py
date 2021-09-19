import pandas as pd
import os
import telegram.bot
import sqlite3
import sys

from rightmove_webscraper import RightmoveData
from sqlite3 import Error

pd.set_option("display.max_rows", 50)
pd.set_option("display.max_columns", 50)

def create_connection(db_file, bot):
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Exception as e:
		exp = f"Exception in connecting to database. Exception: {e}"
		print(exp)
		bot.send_message(chat_id='@housing_ld', text=exp)
		sys.exit(1)
	

''' Create database if not there.'''
def create_table(create_table_sql, connection, bot):
	try:
		connection.execute(create_table_sql)
		connection.commit()
		print("Created table successfully")
		print(create_table_sql)
	except Exception as e:
		exp = f"Exception in creating table. Exception: {e}"
		print(exp)
		bot.send_message(chat_id='@housing_ld', text=exp)
		sys.exit(1)


''' Hit database with return.'''
def select_from_table(sql_command, connection, bot):
	try:
		print(f"Executed command: {sql_command}")
		c = connection.cursor()
		c.execute(sql_command)
		return c.fetchall()
	except Exception as e:
		exp = f"Exception in executing query. Exception: {e}"
		print(exp)
		bot.send_message(chat_id='@housing_ld', text=exp)
		sys.exit(1)

def insert_to_table(sql_command, connection, bot):
	print(f"SQL Command: {sql_command}")
	try:
		cur = connection.cursor()
		cur.execute(sql, task)
		connection.commit()
	except Exception as e:
		exp = f"Exception in inserting query. Exception: {e}"
		print(exp)
		bot.send_message(chat_id='@housing_ld', text=exp)
		sys.exit(1)



if __name__ == "__main__":

	bot = telegram.bot.Bot(token='1981831751:AAHSRDPm850dsKZutlPh_tBH4Kli9Vn6wBA')

	table_name = "housing_ow_q_q_three_bed"


	''' Grab the urls to request '''
	try:
		with open('data/ow_q_q_three_bed.txt','r') as inf:
		    area_url_map = eval(inf.read())
	except Exception as e:
		exp = f"Unable to open file. Exception: {e}"
		print(exp)
		bot.send_message(chat_id='@housing_ld', text=exp)
		sys.exit(1)

	print("read in data")



	''' Set up database connection'''
	conn = create_connection(os.path.realpath('data/db/housing.db'), bot)
	print("conn good")
	sql_create_table_command = f""" 
		CREATE TABLE IF NOT EXISTS {table_name} (
		price integer,
		type text,
		address text,
		url text PRIMARY KEY,
		agent_url text,
		postcode text,
		number_bedrooms integer, 
		area_code text
		); 
	"""
	create_table(sql_create_table_command, conn, bot)
	print('table created')


	''' Cache the urls from the sql table '''
	get_urls_query = f"""SELECT url FROM {table_name}"""
	current_urls = {x[0] for x in select_from_table(get_urls_query, conn, bot)}  # set conversion
	print("url query done")
	print(f"current urls: {current_urls}")

	''' Hit the rightmove API, gather up flats and filter by which we haven't seen'''
	dataframes = []
	for k, v in area_url_map.items():
		try: 
			rm = RightmoveData(v)
		except Exception as e:
			exp = f"Unable to call rightmove api. Exception: {e}"
			print(exp)
			bot.send_message(chat_id='@housing_ld', text=exp)
			sys.exit(1)
		housing_data = rm.get_results.drop(labels='search_date', axis=1)
		housing_data['area_code'] = k
		dataframes.append(housing_data)
	full_housing_dataset = pd.concat(dataframes).drop_duplicates(['url'])
	new_housing_dataset = full_housing_dataset[~full_housing_dataset.url.isin(current_urls)]


	print("new housing found.")
	print(f"Length full dataset: {len(full_housing_dataset)}\nLength of new dataset: {len(new_housing_dataset)}")
	print(new_housing_dataset)
	print(f"columns: {new_housing_dataset.columns}")


	''' Write the new housing to the table. '''
	if len(new_housing_dataset) > 0:
		for index, row in new_housing_dataset.iterrows():
			message = "New Flat:\n" + str(row['area_code']) + " £" + str(row['price']) + "\n" + str(row['url'])
			print(message)
			bot.send_message(chat_id='@housing_ld', text=message)

	new_housing_dataset.to_sql(name=table_name, con=conn, if_exists='append', index=False)







