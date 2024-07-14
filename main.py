from flask import Flask, render_template, jsonify, request, redirect, url_for
from AmazonScraper.spiders.data_parser import DataParserSpider
import time
from scrapy.signalmanager import dispatcher
from scrapy.crawler import CrawlerRunner
from scrapy import signals
import crochet
import os
crochet.setup()

# Creating Flask App Variable

app = Flask(__name__)

output_data = []
crawl_runner = CrawlerRunner()

# By Deafult Flask will come into this when we run the file


@app.route('/')
def index():
    # Returns index.html file in templates folder.
    return render_template("index.html")


# After clicking the Submit Button FLASK will come into this
@app.route('/', methods=['POST'])
def submit():
    if request.method == 'POST':
        s = request.form['url']  # Getting the Input Amazon Product URL
        global baseURL
        baseURL = s

        # This will remove any existing file with the same name so that the scrapy will not append the data to any previous file.
        if os.path.exists("AmazonScraper/outputfile.json"):
            os.remove("AmazonScraper/outputfile.json")

        return redirect(url_for('scrape'))  # Passing to the Scrape function


@app.route("/data")
def scrape():

    # Passing that URL to our Scraping Function
    scrape_with_crochet(baseURL=baseURL)

    time.sleep(10)  # Pause the function while the scrapy spider is running

    # Returns the scraped data after being running for 10 seconds.
    return jsonify(output_data)


@crochet.run_in_reactor
def scrape_with_crochet(baseURL):
    # This will connect to the dispatcher that will kind of loop the code between these two functions.
    dispatcher.connect(_crawler_result, signal=signals.item_scraped)

    # This will connect to the DataParserSpider function in our scrapy file and after each yield will pass to the crawler_result function.
    eventual = crawl_runner.crawl(DataParserSpider, product_url=baseURL)
    return eventual

# This will append the data to the output data list.


def _crawler_result(item, response, spider):
    if item.get('product_name'):
        output_data.append(dict(item))
    else:
        output_data.append(dict({"RESPONSE": "INVALID INPUT"}))


if __name__ == "__main__":
    app.run(debug=True)
