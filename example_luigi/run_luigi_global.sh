#!/bin/bash

# start luigid before running this
python backfill_stock_prices.py GenerateQuandlReport \
  --workers=2 \
  --input-filename ticker_symbols_500.txt \
  --output-filename output/stock_price_500.csv
