import sqlite3
import pandas as pd
import geoip2.database
from geoip2 import errors

conn = sqlite3.connect('dionaea.sqlite')
c = conn.cursor()

def create_df(cursor):
	cursor.execute('SELECT * FROM connections JOIN downloads ON connections.connection = downloads.connection')
	data = cursor.fetchall()
	header = [x[0] for x in cursor.description]
	df = pd.DataFrame(data, columns=header)
	df.drop(columns=['connection', 'connection_type', 'connection_transport', 'connection_protocol', 'connection_root', 'connection_parent', 'download'], inplace=True)
	df['country'] = get_countries(df)
	df.to_csv('dionaea.csv', index=False)

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

create_df(c)
c.close()
conn.close()
