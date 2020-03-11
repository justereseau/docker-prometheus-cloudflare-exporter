#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import json

from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import generate_latest


def process(raw):
    class RegistryMock(object):
        def __init__(self, metrics):
            self.metrics = metrics

        def collect(self):
            for metric in self.metrics:
                yield metric

    def generate_metrics(pop_data, families):
        dns_data = pop_data['dimensions']
        rvalue = pop_data['metrics'][0]

        families['record_queried'].add_metric(dns_data, rvalue)

    families = {
        'record_queried': GaugeMetricFamily(
            'cloudflare_dns_record_queries',
            'DNS queries per record at PoP location.',
            labels=raw['query']["dimensions"]
        )
    }

    for pop_data in raw['data']:
        generate_metrics(pop_data, families)
    return generate_latest(RegistryMock(families.values()))


if __name__ == "__main__":
    import os

    source_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(source_dir, "sample-dns")

    with open(path) as f:
        print( process(json.load(f)['result']) )
