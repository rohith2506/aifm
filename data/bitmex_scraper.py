import os
import logging
import json
import requests
import argparse

logging.basicConfig(level=logging.INFO)

class BitmexScraper:
    def __init__(self, symbol, start_date, start_time, end_date, end_time):
        self.base_url = "https://www.bitmex.com/api/v1/trade/bucketed"
        self.symbol = symbol
        self.start_date = start_date
        self.start_time = start_time
        self.end_date   = end_date
        self.end_time   = end_time
        self.outfile = os.path.join(os.getcwd(), "bitmex", "trade_data_bucketed.json")
        self.construct_uri_params()

    def construct_uri_params(self):
        self.uri_params = {
            "symbol": self.symbol,
            "filter": {
                "startTime": self.start_date + " " + self.start_time,
                "endTime": self.end_date + " " + self.end_time
            },
            "reverse": "false",
            "count": 1000,
            "binSize": "1m",
            "partial": "false"
        }
        self.uri_params["filter"] = json.dumps(self.uri_params["filter"])

    def fetch_data(self):
        try:
            response = requests.get(self.base_url, params = self.uri_params)
            json_data = json.loads(response.text)
            return json_data
        except Exception as err:
            logging.exception("Fetch data failed. Error: {}".format(str(err)))

    def store_data(self, json_data):
        try:
            with open(self.outfile, "w") as ofile:
                ofile.write(json.dumps(json_data))
        except Exception as err:
            logging.exception("Write data failed. Error: {}".format(str(err)))

    def run(self):
        logging.info("Fetching data...")
        json_data = self.fetch_data()
        logging.info("Got {} trades".format(len(json_data)))
        self.store_data(json_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", type=str, required=True, help="Start date")
    parser.add_argument("--start-time", type=str, required=True, help="Start time")
    parser.add_argument("--end-date", type=str, required=True, help="End date")
    parser.add_argument("--end-time", type=str, required=True, help="End time")
    parser.add_argument("--symbol", type=str, required=True, help="Symbol you wanna collect data")

    args = parser.parse_args()
    bitmex_scraper = BitmexScraper(args.symbol, args.start_date, args.start_time, args.end_date, args.end_time)
    bitmex_scraper.run()
