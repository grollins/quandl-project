import pytest
from requests.exceptions import HTTPError
from tap_quandl import parse_response_data, send_request

def test_parse_response_data_to_get_date_and_price():
    response = {'column_names': ['Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Ex-Dividend', 'Split Ratio', 'Adj. Open', 'Adj. High',
                'Adj. Low', 'Adj. Close', 'Adj. Volume'],
                'data': [['2017-06-01', 151.75, 152.29, 150.3, 151.53, 14459173.0,
                          0.0, 1.0, 151.75, 152.29, 150.3, 151.53, 14459173.0],
                         ['2017-06-02', 151.75, 152.29, 150.3, 151.53, 14459173.0,
                          0.0, 1.0, 151.75, 152.29, 150.3, 151.53, 14459173.0]]}
    stock_data = parse_response_data(response)
    assert len(stock_data) == 2
    assert 'date' in stock_data[0]
    assert 'price' in stock_data[0]
    assert 'date' in stock_data[1]
    assert 'price' in stock_data[1]

def test_empty_dict_when_parse_missing_response_data():
    response = {'column_names': ['Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Ex-Dividend', 'Split Ratio', 'Adj. Open', 'Adj. High',
                'Adj. Low', 'Adj. Close', 'Adj. Volume'],
                'data': []}
    stock_data = parse_response_data(response)
    assert len(stock_data) == 0

def test_raise_http_error_when_404():
    with pytest.raises(HTTPError):
        send_request(url='http://httpbin.org/status/404', params={})
