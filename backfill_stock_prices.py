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
            json.dump({'start_date': '2017-01-01', 'end_date': '2017-07-03',
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
    input_filename = luigi.Parameter()
    output_filename = luigi.Parameter()

    def requires(self):
        task_list = []
        with open(self.input_filename, 'r') as f:
            task_list = [SyncPrice(ticker.strip()) for ticker in f.readlines()]
        return task_list

    def output(self):
        return luigi.LocalTarget(self.output_filename)

    def run(self):
        input_filenames = [x.fn for x in self.input()]
        df_list = [pd.read_csv(fn) for fn in input_filenames]
        df = pd.concat(df_list)
        df.to_csv(self.output().fn, index = False)


class GenerateQuandlReport(luigi.Task):
    input_filename = luigi.Parameter()
    output_filename = luigi.Parameter()

    def requires(self):
        return QuandlSync(self.input_filename, self.output_filename)

    def output(self):
        return luigi.LocalTarget('sp500.html')

    def run(self):
        os.system('Rscript generate_report.R')


if __name__ == '__main__':
    luigi.run()
