import json

class Execution(object):

    def __init__(self, json):
        self.json = json

    def status(self):
        return self.json['executionStatus']

    def shard(self):
        return self.json['shardIndex']

    def jobid(self):
        return self.json.get('jobId', None)

    def __str__(self):
        return json.dumps(self.json, sort_keys=True, indent=4, separators=(',', ': '))


class Metadata(object):

    def __init__(self, metadata):
        self.json_doc = metadata

    def calls(self):
        return {
            k: [Execution(x) for x in v] 
            for k, v in self.json_doc['calls'].iteritems()
            }
