import sqlite3
from sqlite3 import Error
import os

def create_connection(db_file):
	conn = None
	try:
		conn = sqlite3.connect(db_file)
		print(sqlite3.version)
		return conn
	except Error as e:
		print(e)
	

# Create database if not there.
def create_table(create_table_sql, connection):
	try:
		connection.execute(create_table_sql)
		connection.commit()
		print("Created table successfully")
		print(create_table_sql)
	except Error as e:
		print(e)


# Hit database with return.
def command_to_table(sql_command, connection):
	try:
		print(f"Executed command: {sql_command}")
		c = connection.cursor()
		c.execute(sql_command)
		
		return c.fetchall()
	except Error as e:
		print(e)

		



if __name__ == '__main__':

	table_name = "housing_ow_q_q_three_bed"

	conn = create_connection(os.path.realpath('data/db/housing.db'))

	sql_create_table_command = f""" 
		CREATE TABLE IF NOT EXISTS {table_name} (
		price integer,
		type text,
		url text PRIMARY KEY,
		agent_url text,
		postcode text,
		number_bedrooms integer, 
		area_code text
		); 
	"""

	create_table(sql_create_table_command, conn)

	s = "blah"
	command_to_table(f"""INSERT INTO {table_name} (price, type, url, agent_url, postcode, number_bedrooms, area_code) VALUES (1, "blah", "blah", "blah", "blah", 1, "blah");""", conn)
	command_to_table(f"""INSERT INTO {table_name} (price, type, url, agent_url, postcode, number_bedrooms, area_code) VALUES (2, "blah2", "blah2", "blah2", "blah2", 1, "blah");""", conn)

	# Get all the urls from the database, and form these into a set.
	get_urls = f"""
		SELECT url FROM {table_name}
	"""
	urls = {x[0] for x in command_to_table(get_urls, conn)}


	df[~df.country.isin(urls)]

	
	print(type(urls))
	print(urls)
	













