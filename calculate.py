from __future__ import division
import dateutil.parser
import math


class Disk(object):

    def __init__(self, size, disk_type='PERSISTENT_HDD'):
        self.size = size
        self.type_ = disk_type


class GenomicsOperation(object):

    def __init__(self, response_json):
        meta = response_json['metadata']
        gce = meta['runtimeMetadata']['computeEngine']
        self.machine = gce['machineType'].split('/')[1]
        self.zone = gce['zone']
        self.region, _ = self.zone.rsplit('-', 1)
        resources_dict = meta['request']['pipelineArgs']['resources']
        self.preemptible = resources_dict['preemptible']
        self.start_time = dateutil.parser.parse(
            meta['startTime']
            )

        try:
            self.end_time = dateutil.parser.parse(meta['endTime'])
            self.length = self.end_time - self.start_time
        except KeyError:
            self.end_time = None
            self.duration = None

        self.disks = [
            Disk(x['sizeGb'], x['type']) for x in resources_dict['disks']
            ]
        self.disks.append(Disk(resources_dict['bootDiskSizeGb']))

    def duration(self):
        if self.length:
            return self.length.total_seconds()
        else:
            return None


class OperationCostCalculator(object):

    def __init__(self, pricelist_json):
        self.pricelist_json = pricelist_json

    def cost(self, operation):
        region = operation.region
        machine = operation.machine
        preemptible = operation.preemptible
        duration = operation.duration()
        cost = self.resource_cost(
            self.vm_duration(duration),
            self.price(
                self.machine_name_to_resource(machine, preemptible),
                region
                )
            )
        for d in operation.disks:
            cost = cost + self.resource_cost(
                self.disk_duration(duration),
                self.price(
                    self.disk_type_to_resource(d.type_),
                    region
                    ),
                d.size
                )
        return cost

    def price(self, resource, region):
        return self.pricelist_json['gcp_price_list'][resource][region]

    def resource_cost(self, duration, price, units=1):
        return duration * price * units

    @staticmethod
    def machine_name_to_resource(name, premptible):
        lineitem = 'CP-COMPUTEENGINE-VMIMAGE-{0}'.format(name.upper())
        if premptible:
            lineitem = lineitem + '-PREEMPTIBLE'
        return lineitem

    @staticmethod
    def disk_type_to_resource(type_):
        lineitem = 'CP-COMPUTEENGINE-STORAGE-PD-{0}'
        if type_ == 'PERSISTENT_HDD':
            disk_code = 'CAPACITY'
        elif type_ == 'PERSISTENT_SSD':
            disk_code = 'SSD'
        else:
            raise RuntimeError('Unknown disk type: {0}'.format(type_))
        return lineitem.format(disk_code)

    @staticmethod
    def vm_duration(duration):
        minutes = duration / 60.0
        if minutes < 10:  # Enforce minimum of 10 minutes
            price_duration = 10
        else:
            price_duration = math.ceil(minutes)  # round up to nearest minute
        return price_duration / 60  # convert to hours to match price

    @staticmethod
    def disk_duration(duration):
        # convert to months. Assuming a 30.5 day month or 732 hours.
        # rounding up the seconds. Not sure if necessary
        return math.ceil(duration) / 60 / 60 / 24 / 30.5
