from os import environ
import requests

TOKEN = environ['QUANDL_TOKEN']

param_dict = {'api_key': TOKEN,
              'start_date': '2017-06-01',
              'end_date': '2017-06-01',
              'order': 'asc'}
url = 'https://www.quandl.com/api/v3/datasets/WIKI/FB/data.json'
r = requests.get(url, params=param_dict)
print(r.json())

payload = r.json()['dataset_data']

date_idx = payload['column_names'].index('Date')
adj_close_idx = payload['column_names'].index('Adj. Close')

dates = [d[date_idx] for d in payload['data']]
adj_close_prices = [d[adj_close_idx] for d in payload['data']]
