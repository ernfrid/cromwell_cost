from __future__ import division
import dateutil.parser
import math
from collections import namedtuple


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
            self.length = None

        self.disks = [
            Disk(x['sizeGb'], x['type']) for x in resources_dict['disks']
            ]
        self.disks.append(Disk(resources_dict['bootDiskSizeGb']))

    def duration(self):
        if self.length:
            return self.length.total_seconds()
        else:
            return None

    def __str__(self):
        return ("(machine: {}, "
                "zone: {}, "
                "region: {}, "
                "preemptible: {}, "
                "start_time: {}, "
                "end_time: {}, "
                "length: {}, "
                "duration: {} )").format(
                        self.machine,
                        self.zone,
                        self.region,
                        self.preemptible,
                        self.start_time,
                        self.end_time,
                        self.length,
                        self.duration()
                        )


Resource = namedtuple('Resource', ['duration', 'region', 'name', 'units'])


def vm_resource_name(name, premptible):
    identifier = 'CP-COMPUTEENGINE-VMIMAGE-{0}'.format(name.upper())
    if premptible:
        identifier = identifier + '-PREEMPTIBLE'
    return identifier


def disk_resource_name(type_):
    lineitem = 'CP-COMPUTEENGINE-STORAGE-PD-{0}'
    if type_ == 'PERSISTENT_HDD':
        disk_code = 'CAPACITY'
    elif type_ == 'PERSISTENT_SSD':
        disk_code = 'SSD'
    else:
        raise RuntimeError('Unknown disk type: {0}'.format(type_))
    return lineitem.format(disk_code)


def vm_duration(duration):
    '''Return the duration in hours with a minimum of 1 minutes.'''
    # All machine types are charged a minimum of 10 minutes.
    # See https://cloud.google.com/compute/pricing#billingmodel
    if duration < 60:  # Enforce minimum of 1 minutes
        price_duration = 60
    return duration / 60.0 / 60.0


def disk_lifetime(duration):
    '''Return the duration in months based on a 30.5 day month.'''
    # convert to months. Assuming a 30.5 day month or 732 hours based on
    # footnote here: https://cloud.google.com/compute/pricing#localssdpricing
    # rounding up the seconds based on https://cloud.google.com/compute/pricing#disk.
    # Not sure if necessary depending on how timing is done in operations
    # NOTE A 730 hour month may be more appropriate based on the above footnote
    # and https://cloud.google.com/compute/pricing#billingmodel
    return math.ceil(duration) / 60 / 60 / 24 / 30.5


def vm_resource(op):
    return Resource(
            duration=vm_duration(op.duration()),
            region=op.region,
            name=vm_resource_name(op.machine, op.preemptible),
            units=1,
            )


def disk_resources(op):
    return [Resource(
                duration=disk_lifetime(op.duration()),
                region=op.region,
                name=disk_resource_name(d.type_),
                units=d.size,
                ) for d in op.disks
            ]


def as_resources(op):
    resources = disk_resources(op)
    resources.append(vm_resource(op))
    return resources


class OperationCostCalculator(object):

    def __init__(self, pricelist_json):
        self.pricelist_json = pricelist_json

    def cost(self, operation):
        return sum([self.resource_cost(x) for x in as_resources(operation)])

    def price(self, resource):
        return self.pricelist_json['gcp_price_list'][resource.name][resource.region]

    def resource_cost(self, resource):
        return resource.duration * self.price(resource) * resource.units
