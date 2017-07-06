#!/bin/bash

python backfill_stock_prices.py QuandlSync \
  --local-scheduler \
  --workers=2 \
  --input-filename ticker_symbols.txt \
  --output-filename output/stock_price.csv
