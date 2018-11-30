import sys
from collections import defaultdict
import argparse
from intervaltree import Interval, IntervalTree

import otf2
from otf2.enums import Type

from metricdict import MetricDict
from spacecollection import AccessType, AccessSequence, AddressSpace, Access


class MemoryAccessStatistics:
    """
    Stores access statistics of all utilized address spaces.
    """

    def __init__(self):
        self._address_spaces = IntervalTree()


    def add_mapped_space(self, space):
        self._address_spaces.addi(space.Address,
                                  space.Address + space.Size,
                                  space)


    def add_access(self, event, location):
        intervals = self._address_spaces[int(event.value)]
        assert(len(intervals) < 2)
        if len(intervals) == 1:
            address = int(event.value)
            access_type = AccessType.get_by_name(event.metric.member.name)
            intervals.pop().data.add_access_on_location(event.time,
                                                        Access(address, access_type),
                                                        location)


    def create_access_metrics(self, trace_writer):
        async_metrics = MetricDict(trace_writer)
        for space in self._address_spaces:
            for location, access_seq in space.data.get_all_accesses():
                metric_name = "Access:{}".format(space.data.Source)

                metric_key = "{}:{}".format(space.data.Source, str(location.name))

                metric = async_metrics.get(location, metric_name, metric_key, unit="address", value_type=Type.UINT64)

                writer = trace_writer.event_writer_from_location(metric.location)

                for t, a in access_seq.get():
                    writer.metric(t, metric.instance, a.address)


    def create_counter_metrics(self, trace_writer):

        def _get_counter(async_metrics, source, prefix, location, unit="#"):
            metric_name = "{}:{}".format(prefix, source)
            metric_key = "{}:{}".format(metric_name, str(location.name))
            metric = async_metrics.get(location, metric_name, metric_key, unit=unit, value_type=Type.UINT64)
            writer = trace_writer.event_writer_from_location(metric.location)
            return metric, writer, metric_key

        async_metrics = MetricDict(trace_writer)
        counters = defaultdict(int)
        for space in self._address_spaces:
            for location, access_seq in space.data.get_all_accesses():
                (load_metric, load_writer, load_key) = _get_counter(async_metrics, space.data.Source, "LoadCounter", location)
                (store_metric, store_writer, store_key) = _get_counter(async_metrics, space.data.Source, "StoreCounter", location)
                for t, a in access_seq.get():
                    if a.type == AccessType.LOAD:
                        counters[load_key] += 1
                        load_writer.metric(t, load_metric.instance, counters[load_key])
                    elif a.type == AccessType.STORE:
                        counters[store_key] += 1
                        store_writer.metric(t, store_metric.instance, counters[store_key])
                    else:
                        print("Found invalid access type.", file=sys.stderr)


    def get_space_stats(self):
        stats = defaultdict(list)
        for space in self._address_spaces:
            stats[space.data.Source].append(space.data)
        return stats


    def __str__(self):
        out = ""
        for space in self._address_spaces:
            for loc, seq in space.data.get_all_accesses():
                out += "{}\n\n".format(seq)
        return out