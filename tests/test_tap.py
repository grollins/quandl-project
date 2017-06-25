import pytest
from requests.exceptions import HTTPError
from tap_quandl import parse_response_data, send_request

def test_parse_response_data_to_get_date_and_price():
    response = {'column_names': ['Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Ex-Dividend', 'Split Ratio', 'Adj. Open', 'Adj. High',
                'Adj. Low', 'Adj. Close', 'Adj. Volume'],
                'data': [['2017-06-01', 151.75, 152.29, 150.3, 151.53, 14459173.0,
                          0.0, 1.0, 151.75, 152.29, 150.3, 151.53, 14459173.0]]}
    data_dict = parse_response_data(response)
    assert 'date' in data_dict
    assert 'price' in data_dict

def test_raise_http_error_when_404():
    with pytest.raises(HTTPError):
        send_request(url='http://httpbin.org/status/404', params={})
