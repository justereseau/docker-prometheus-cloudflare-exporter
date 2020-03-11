# -*- encoding: utf-8 -*-

from __future__ import print_function

import datetime
import delorean
import os
import sys
import json
import logging

import requests
import time

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import generate_latest

from . import coloexporter
from . import dnsexporter
from . import wafexporter

logging.basicConfig(level=logging.os.environ.get('LOG_LEVEL', 'INFO'))

REQUIRED_VARS = {'AUTH_KEY', 'ZONE'}
for key in REQUIRED_VARS:
    if key not in os.environ:
        logging.error('Missing value for %s' % key)
        sys.exit()

SERVICE_PORT = int(os.environ.get('SERVICE_PORT', 9199))
ZONE = os.environ.get('ZONE')
ENDPOINT = 'https://api.cloudflare.com/client/v4/'
AUTH_KEY = os.environ.get('AUTH_KEY')
HEADERS = {
    'Authorization': 'Bearer ' + AUTH_KEY,
    'Content-Type': 'application/json'
}
HTTP_SESSION = requests.Session()


class RegistryMock(object):
    def __init__(self, metrics):
        self.metrics = metrics

    def collect(self):
        for metric in self.metrics:
            yield metric


def get_data_from_cf(url):
    r = HTTP_SESSION.get(url, headers=HEADERS)
    return json.loads(r.content.decode('UTF-8'))


def get_zone_id():
    r = get_data_from_cf(url='%szones?name=%s' % (ENDPOINT, ZONE))
    return r['result'][0]['id']


def metric_processing_time(name):
    def decorator(func):
        # @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            result = func(*args, **kwargs)
            elapsed = (time.time() - now) * 1000
            logging.debug('Processing %s took %s miliseconds' % (
                name, elapsed))
            internal_metrics['processing_time'].add_metric([name], elapsed)
            return result
        return wrapper
    return decorator

@metric_processing_time('waf')
def get_waf_metrics():

    time_since = (
                    datetime.datetime.now() + datetime.timedelta(minutes=-1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_until = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    path_format = '%szones/%s/security/events?kind=firewall&per_page=50%s'
    path_format += '&since=%s'
    path_format += '&until=%s'

    zone_id = get_zone_id()

    records = []
    next_page_id = ''
    is_next_page = True

    while is_next_page:
        url = path_format % (ENDPOINT, zone_id, next_page_id, time_since, time_until)
        r = get_data_from_cf(url=url)

        if 'success' not in r or not r['success']:
            logging.error('Failed to get information from Cloudflare')
            for error in r['errors']:
                logging.error('[%s] %s' % (error['code'], error['message']))
                return ''

        if not r['result']:
            is_next_page = False

        next_page_id = ('&cursor=%s' % r['result_info']['cursors']['after'])
        
        logging.info('Page finished. cursor=%s' % r['result_info']['cursors']['after'])

        records.append(r['result'])

    return wafexporter.process(records)

@metric_processing_time('dns')
def get_dns_metrics():
    logging.info('Fetching DNS metrics data')
    time_since = (
                    datetime.datetime.now() + datetime.timedelta(minutes=-1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_until = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    endpoint = '%szones/%s/dns_analytics/report?metrics=queryCount'
    endpoint += '&dimensions=queryName,responseCode,origin,tcp,ipVersion'
    endpoint += '&since=%s'
    endpoint += '&until=%s'

    logging.info('Using: since %s until %s' % (time_since, time_until))
    r = get_data_from_cf(url=endpoint % (ENDPOINT, get_zone_id(), time_since, time_until))

    if not r['success']:
        logging.error('Failed to get information from Cloudflare')
        for error in r['errors']:
            logging.error('[%s] %s' % (error['code'], error['message']))
            return ''

    records = int(r['result']['rows'])
    logging.info('Records retrieved: %d' % records)
    if records < 1:
        return ''
    return dnsexporter.process(r['result'])


def update_latest():
    global latest_metrics, internal_metrics
    internal_metrics = {
        'processing_time': GaugeMetricFamily(
            'cloudflare_exporter_processing_time_miliseconds',
            'Processing time in ms',
            labels=[
                'name'
            ]
        )
    }

    latest_metrics = (get_dns_metrics() + get_waf_metrics())
    latest_metrics += generate_latest(RegistryMock(internal_metrics.values()))


app = Flask(__name__)


@app.route("/")
def home():
    return """<h3>Welcome to the Cloudflare prometheus exporter!</h3>
The following endpoints are available:<br/>
<a href="/metrics">/metrics</a> - Prometheus metrics<br/>
<a href="/status">/status</a> - A simple status endpoint returning "OK"<br/>"""


@app.route("/status")
def status():
    return "OK"


@app.route("/metrics")
def metrics():
    return latest_metrics


def run():
    logging.info('Starting scrape service for zone "%s" using key [%s...]'
                 % (ZONE, AUTH_KEY[0:6]))

    update_latest()

    scheduler = BackgroundScheduler({'apscheduler.timezone': 'UTC'})
    scheduler.add_job(update_latest, 'interval', seconds=60)
    scheduler.start()

    try:
        app.run(host="0.0.0.0", port=SERVICE_PORT, threaded=True)
    finally:
        scheduler.shutdown()
