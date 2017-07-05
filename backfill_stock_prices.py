import os
import json
import luigi
import pandas as pd


class MakeTapConfig(luigi.Task):
    ticker = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('config/%s.json' % self.ticker)

    def run(self):
        with self.output().open('w') as f:
            json.dump({'start_date': '2017-05-01', 'end_date': '2017-06-03',
                       'ticker': self.ticker}, f)


class SyncPrice(luigi.Task):
    ticker = luigi.Parameter()

    def requires(self):
        return MakeTapConfig(ticker=self.ticker)

    def output(self):
        return luigi.LocalTarget('output/%s.csv' % self.ticker)

    def run(self):
        tap_cmd = 'tap-quandl-stock-price -c %s' % self.input().fn
        target_cmd = 'target-csv -c csv_config.json -o %s' % self.output().fn
        os.system('%s | %s' % (tap_cmd, target_cmd))


class QuandlSync(luigi.Task):

    def requires(self):
        task_list = []
        with open('ticker_symbols.txt', 'r') as f:
            task_list = [SyncPrice(ticker.strip()) for ticker in f.readlines()]
        return task_list

    def output(self):
        return luigi.LocalTarget('output/stock_price.csv')

    def run(self):
        input_filenames = [x.fn for x in self.input()]
        df_list = [pd.read_csv(fn) for fn in input_filenames]
        df = pd.concat(df_list)
        df.to_csv(self.output().fn, index = False)

if __name__ == '__main__':
    luigi.run()
