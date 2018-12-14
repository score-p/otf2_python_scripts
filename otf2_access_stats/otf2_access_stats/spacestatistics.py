import sys
from collections import defaultdict
import operator
from functools import reduce
from intervaltree import IntervalTree

from otf2.enums import Type

from .metricdict import MetricDict
from .spacecollection import AccessType, Access, Flush, TimeStamp, count_loads


def format_byte(num):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "{} {}Byte".format(num, unit)
        num /= 1024.0
    return "{} YiByte".format(num)


class MemoryAccessStatistics:
    """
    Stores access statistics of all utilized address spaces.
    """

    def __init__(self, resolution):
        self._address_spaces = IntervalTree()
        self._stats_per_source = defaultdict(list)
        self._clock_resolution = resolution
        self._flushs_per_source = defaultdict(list)


    def add_mapped_space(self, space):
        self._address_spaces.addi(space.start_address,
                                  space.start_address + space.size,
                                  space)
        self._stats_per_source[space.source].append(space)


    def add_access(self, event, location):
        intervals = self._address_spaces[int(event.value)]
        assert len(intervals) < 2
        if len(intervals) == 1:
            address = int(event.value)
            access_type = AccessType.get_by_name(event.metric.member.name)
            intervals.pop().data.add_access_on_location(event.time,
                                                        Access(address, access_type),
                                                        location)


    def add_flush(self, enter_event, leave_event):
        t_begin = TimeStamp(self._clock_resolution, enter_event.time)
        t_end = TimeStamp(self._clock_resolution, leave_event.time)
        flush = Flush(leave_event.attributes, t_begin, t_end)
        for interval in self._address_spaces[flush.interval().begin:flush.interval().end]:
            interval.data.flush(flush)
            self._flushs_per_source[interval.data.source].append(flush)


    def create_access_metrics(self, trace_writer):
        async_metrics = MetricDict(trace_writer)
        for space in self._address_spaces:
            for location, access_seq in space.data.get_all_accesses():
                metric_name = "Access:{}".format(space.data.source)

                metric_key = "{}:{}".format(space.data.source, str(location.name))

                metric = async_metrics.get(location, metric_name, metric_key,
                                           unit="address", value_type=Type.UINT64)

                writer = trace_writer.event_writer_from_location(metric.location)

                for time, access in access_seq.get():
                    writer.metric(time, metric.instance, access.address)


    def create_counter_metrics(self, trace_writer):

        def _get_counter(async_metrics, source, prefix, location, unit="#"):
            metric_name = "{}:{}".format(prefix, source)
            metric_key = "{}:{}".format(metric_name, str(location.name))
            metric = async_metrics.get(location, metric_name, metric_key,
                                       unit=unit, value_type=Type.UINT64)
            writer = trace_writer.event_writer_from_location(metric.location)
            return metric, writer, metric_key

        async_metrics = MetricDict(trace_writer)
        counters = defaultdict(int)
        for space in self._address_spaces:
            for location, access_seq in space.data.get_all_accesses():
                (load_metric, load_writer, load_key) = _get_counter(async_metrics,
                                                                    space.data.source,
                                                                    "LoadCounter",
                                                                    location)
                (store_metric, store_writer, store_key) = _get_counter(async_metrics,
                                                                       space.data.source,
                                                                       "StoreCounter",
                                                                       location)
                for time, access in access_seq.get():
                    if access.type == AccessType.LOAD:
                        counters[load_key] += 1
                        load_writer.metric(time, load_metric.instance, counters[load_key])
                    elif access.type == AccessType.STORE:
                        counters[store_key] += 1
                        store_writer.metric(time, store_metric.instance, counters[store_key])
                    else:
                        print("Found invalid access type.", file=sys.stderr)


    def get_space_stats(self):
        stats = defaultdict(list)
        for space in self._address_spaces:
            stats[space.data.source].append(space.data)
        return stats


    def _all_resource_utilizations(self):
        src_util = defaultdict(int)
        for key, allocations in self._stats_per_source.items():
            for space in allocations:
                src_util[key] = sum([len(access_seq) for _, access_seq in space.get_all_accesses()])
        return src_util


    def resource_utilization(self, location=None):
        if not location:
            return self._all_resource_utilizations()
        source_util = defaultdict(int)
        for space_name, allocations in self._stats_per_source.items():
            source_util[space_name] = sum([len(allocation[location]) for allocation in allocations])
        return source_util


    def accumulated_flush_time(self, resource):
        return reduce(operator.add, [flush.duration() for flush
                                     in self._flushs_per_source[resource]])


    def resource_summary(self, resource):
        summary = dict()
        summary["Number of Allocations"] = len(self._stats_per_source[resource])
        summary["Time spent in Flush"] = str(self.accumulated_flush_time(resource))
        flushed_range = 0
        alloc_data = 0
        flush_data = 0
        for allocation in self._stats_per_source[resource]:
            flush_data += allocation.flushed_data
            alloc_data += allocation.size

            flush_tree = IntervalTree()
            for interval in allocation.flushs():
                flush_tree.add(interval)
            flush_tree.merge_overlaps()
            flushed_range += sum([i.length() for i in flush_tree])

        summary["Flushed Data"] = format_byte(flush_data)
        summary["Allocated Memory"] = format_byte(alloc_data)

        if flushed_range > 0:
            summary["Flush Coverage"] = "{} %".format((flushed_range / alloc_data) * 100)

        return summary


    def thread_access_stats(self, resource):
        load_distribution = defaultdict(int)
        store_distribution = defaultdict(int)
        for space in self._stats_per_source[resource]:
            for loc, seq in space.get_all_accesses():
                loads = count_loads(seq)
                stores = len(seq) - loads
                load_distribution["Sum"] += loads
                store_distribution["Sum"] += stores
                load_distribution[loc.name] += loads
                store_distribution[loc.name] += stores
        return (store_distribution, load_distribution)


    def __str__(self):
        out = ""
        for space in self._address_spaces:
            for _, seq in space.data.get_all_accesses():
                out += "{}\n\n".format(seq)
        return out
