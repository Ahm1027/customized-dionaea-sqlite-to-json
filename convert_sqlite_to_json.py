import sqlite3
import pandas as pd
import geoip2.database
from geoip2 import errors
import os

def convert_timestamp(ip):
	return datetime.datetime.fromtimestamp(pd.to_numeric(ip))

def create_df(cursor):
	cursor.execute('SELECT * FROM connections JOIN downloads ON connections.connection = downloads.connection')
	data = cursor.fetchall()
	header = [x[0] for x in cursor.description]
	df = pd.DataFrame(data, columns=header)
	if os.path.exists('./position.txt'):
		records_already_read = 0
		with open('./position.txt', 'r+') as f:
			records_already_read = f.read()
			f.seek(0)
			f.write(records_already_read)
			df = df.iloc[records_already_read-1:]
			f.truncate()
	else:
		with open('position.txt', 'w') as w:
			w.write(len(df))

	df['connection_timestamp'] = df.loc[:,'connection_timestamp'].apply(convert_timestamp)

	try:
		df.drop(columns=['connection_type', 'connection_transport', 'connection_protocol', 'connection_root', 'connection_parent', 'download', 'remote_hostname'], inplace=True)
	except KeyError e:
		print('df keys not found')

	df['country'] = get_countries(df)
	if os.path.exists('./dionaea_output.json'):
		with open('dionaea_output.json', 'a') as a:
			for x in df.iterrows():
				a.write(json.dumps(x[1].to_dict())+"\n")
		return
	with open('dionaea_output.json', 'w') as w:
		for x in df.iterrows():
			w.write(json.dumps(x[1].to_dict())+"\n")

def get_countries(df):
	countries = []
	with geoip2.database.Reader('./geoip_data/GeoLite2-Country.mmdb') as r:
		for x in df.iterrows():
			try:
				response_city = r.country(x[1]['remote_host'])
				countries.append(response_city.country.name)
			except errors.AddressNotFoundError:
				countries.append('Others')
				continue
	return countries


conn = sqlite3.connect('dionaea.sqlite')
c = conn.cursor()
create_df(c)
c.close()
conn.close()
