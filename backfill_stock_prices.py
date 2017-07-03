import luigi

class SyncPrice(luigi.Task):
    ticker = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("{}.csv".template(ticker))

    def run(self):
        # write config
        config_file = '{}_config.json'.template(ticker)
        with open(config_file, 'w') as f:
            json.dumps({'start_date': '2017-06-01', 'end_date': '2017-06-03',
                        'ticker': ticker})
        sys.cmd('tap-quandl-stock-price -c {} | target_csv -c csv_config.json'.template(config_file))

class SquaredNumbers(luigi.Task):

    def requires(self):
        return [SyncPrice(ticker='FB'), SyncPrice(ticker='SBUX')]

    def output(self):
        return luigi.LocalTarget("stock_price.csv")

    def run(self):
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out))

if __name__ == '__main__':
    luigi.run()
