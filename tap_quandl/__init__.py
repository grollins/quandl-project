#!/usr/bin/env python3

import os
import json
import sys
import argparse
import time
import requests
import singer
import backoff

from datetime import date, datetime, timedelta

TOKEN = os.environ['QUANDL_TOKEN']

base_url = 'https://www.quandl.com/api/v3/datasets/WIKI/'

logger = singer.get_logger()
session = requests.Session()

DATE_FORMAT = '%Y-%m-%d'

def parse_response_data(response):
    date_idx = response['column_names'].index('Date')
    adj_close_idx = response['column_names'].index('Adj. Close')

    data = []
    if len(response['data']) > 0:
        data = [dict(date=x[date_idx], price=x[adj_close_idx]) for x in response['data']]
    return data

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

schema = singer.utils.load_json(get_abs_path('schema.json'))

def giveup(error):
    logger.error(error.response.text)
    response = error.response
    # keep trying if the response code is one of these
    return not (response.status_code == 429 or
                response.status_code >= 500)

@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def send_request(url, params):
    response = session.get(url=url, params=params)
    response.raise_for_status()
    return response

def do_sync(ticker, start_date, end_date):
    template_str = 'Collecting {} stock prices from Quandl for {} to {}'
    logger.info(template_str.format(ticker, start_date, end_date))
    singer.write_schema('stock_price', schema, 'date')

    if datetime.strptime(start_date, DATE_FORMAT) > datetime.utcnow():
        return
    elif datetime.strptime(start_date, DATE_FORMAT) > \
         datetime.strptime(end_date, DATE_FORMAT):
        return

    state = {'start_date': start_date}

    ticker_url = '/'.join([base_url, ticker, 'data.json'])

    try:
        param_dict = {'api_key': TOKEN,
                      'start_date': start_date,
                      'end_date': end_date,
                      'order': 'asc'}
        response = send_request(ticker_url, param_dict)

    except requests.exceptions.RequestException as e:
        logger.fatal('Error on ' + e.request.url +
                     '; received status ' + str(e.response.status_code) +
                     ': ' + e.response.text)
        singer.write_state(state)
        sys.exit(-1)

    stock_data = parse_response_data(response.json()['dataset_data'])
    for d in stock_data:
        if 'price' in d:
            d['ticker'] = ticker
            singer.write_records('stock_price', [d])
            next_date = (datetime.strptime(d['date'], DATE_FORMAT) + \
                             timedelta(days=1)).strftime(DATE_FORMAT)
            state['start_date'] = next_date
        else:
            # data_dict will be empty if the markets were closed on
            # next_date
            continue

    singer.write_state(state)
    logger.info('Tap exiting normally')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=False)
    parser.add_argument(
        '-s', '--state', help='State file', required=False)

    args = parser.parse_args()

    if args.config:
        with open(args.config) as file:
            config = json.load(file)
    else:
        config = {}

    if args.state:
        with open(args.state) as file:
            state = json.load(file)
    else:
        state = {}

    start_date = state.get('start_date',
                            config.get('start_date', '2017-06-01'))

    end_date = state.get('end_date',
                            config.get('end_date', '2017-06-02'))

    do_sync(config.get('ticker', 'SBUX'), start_date, end_date)


if __name__ == '__main__':
    main()
