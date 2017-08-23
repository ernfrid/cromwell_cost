from __future__ import division
from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from gcloud import GenomicsOperation, OperationCostCalculator
from cromwell import Metadata
from collections import defaultdict

import json
import sys
import math
import argparse


class CromwellCostCalculator(object):

    def __init__(self, pricelist):
        credentials = GoogleCredentials.get_application_default()
        self.service = discovery.build('genomics', 'v1', credentials=credentials)
        self.calculator = OperationCostCalculator(pricelist)

    def get_operation_metadata(self, name):
        request = self.service.operations().get(name=name)
        response = request.execute()
        return response

    @staticmethod
    def dollars(raw_cost):
        return math.ceil(raw_cost * 100) / 100

    def calculate_cost(self, metadata_json):
        metadata = Metadata(metadata_json)

        total_cost = 0
        max_shard = -1
        summary_json = { 'tasks': [], 'total_cost': None, 'cost_per_shard': None }

        for task, executions in metadata.calls().iteritems():
            task_totals = defaultdict(int)
            for e in executions:
                op = GenomicsOperation(self.get_operation_metadata(e.jobid()))
                print "operation: {}".format(op)
                task_totals[e.shard()] = task_totals[e.shard()] + self.dollars(self.calculator.cost(op))
                total_cost += self.dollars(self.calculator.cost(op))
            summary_json['tasks'].append({
                    'name': task,
                    'shards': len(task_totals),
                    'cost_per_shard': self.dollars(sum(task_totals.values())/len(task_totals)),
                    'total_cost': self.dollars(sum(task_totals.values()))
                    })
            max_shard = max(max_shard, len(task_totals))
        summary_json['total_cost'] = total_cost
        summary_json['cost_per_shard'] = total_cost / max_shard
        return summary_json

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("pricelist", type=argparse.FileType('r'), help="pricelist.json from Google containing cost information")
    parser.add_argument("metadata", type=argparse.FileType('r'), help="metadata from a cromwell workflow from which to estimate cost")
    args = parser.parse_args()
    metadata = json.load(args.metadata)
    pricelist = json.load(args.pricelist)

    calc = CromwellCostCalculator(pricelist)
    cost = calc.calculate_cost(metadata)
    print json.dumps(cost, sort_keys=True, indent=4)
    print 'Total: ${0}'.format(cost['total_cost'])
    print 'Per Shard: ${0}'.format(cost['cost_per_shard'])


