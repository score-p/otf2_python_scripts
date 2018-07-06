#! /usr/bin/env python3
import sys
import os.path
import otf2
# TODO Really needed?
import collections
from collections import defaultdict
import math
import json
import argparse
from enum import Enum, auto
from intervaltree import Interval, IntervalTree


MMAP_SIZE_TAG = "mappedSize"
MMAP_ADDRESS_TAG = "startAddress"
MMAP_SOURCE_TAG = "mappedSource"
SCOREP_MEMORY_ADDRESS = "scorep:memoryaddress:begin"
SCOREP_MEMORY_SIZE = "scorep:memoryaddress:len"


class AddressSpace:
    """
    Stores details of used address space.
    """

    def __init__(self):
        self.Size = -1
        self.Source = ""
        self.Address = -1

    def __str__(self):
        return "[{}, {}] = Size: {}, Source {}, {}".format(
            self.Address,
                                                            self.Address + self.Size,
                                                            self.Size,
            self.Source)


class AccessType (Enum):
    """
    Enum class to distinguish between access types.
    """

    LOAD = auto()
    STORE = auto()
    INVALID = auto()

    @classmethod
    def get_access_type(cls, type_name):
        if type_name == "MemoryAccess:load":
            return cls.LOAD
        elif type_name == "MemoryAccess:store":
            return cls.STORE
        return cls.INVALID

    @classmethod
    def contains(cls, type_name):
        return cls.get_access_type(type_name) != cls.INVALID


class AccessMetric:
    """
    Extends the OTF2 definitions.metric.
    """

    def __init__(self, trace_writer, event_writer, name, timestamp):
        self._name = name
        self._count = 0
        self._event_writer = event_writer
        self._metric = trace_writer.definitions.metric("{}".format(name),
                                                        unit="Number of Accesses")
        self._event_writer.metric(timestamp, self._metric, 0)

    def inc(self, timestamp):
        self._count += 1
        self._event_writer.metric(timestamp, self._metric, self._count)

    def __str__(self):
        return "{} : {}".format(self._name, self._count)


class AddressSpaceStatistic:
    """
    Provides access statistics for a address space.
    """

    def __init__(self, space, trace, timestamp):
        self._space = space
        self._load_metric = defaultdict(AccessMetric)
        self._store_metric = defaultdict(AccessMetric)
        for loc in trace.definitions.locations:
            if loc.type == otf2.LocationType.CPU_THREAD:
                event_writer = trace.event_writer_from_location(loc)
                self._load_metric[loc] = AccessMetric(trace, event_writer, "{}:Load".format(self._space.Source), timestamp)
                self._store_metric[loc] = AccessMetric(trace, event_writer, "{}:Store".format(self._space.Source), timestamp)

    def inc_metric(self, location, metric_name, timestamp):
        if AccessType.get_access_type(metric_name) == AccessType.LOAD:
            self._load_metric[location].inc(timestamp)
        elif AccessType.get_access_type(metric_name) == AccessType.STORE:
            self._store_metric[location].inc(timestamp)

    def __str__(self):
        return "{} {} {}".format(self._space, self._load_metric, self._store_metric)


class MemoryMappedIo:
    """
    Holds statistics of all used address spaces.
    It uses an IntervalTree where an Interval refers
    to an instance of AddressSpaceStatistic.
    """

    def __init__(self):
        self._address_spaces = IntervalTree()
        self._number_of_accesses = 0

    def add_mapped_space(self, location, space, timestamp, trace):
        if space:
            self._address_spaces.addi(space.Address,
                                     space.Address + space.Size,
                                     AddressSpaceStatistic(space,
                                                     trace,
                                                     timestamp))

    def add_access(self, event, location):
        self._number_of_accesses += 1
        intervals = self._address_spaces[int(event.value)]
        assert(len(intervals) < 2)
        if len(intervals) == 1:
            space_stats = intervals.pop().data
            space_stats.inc_metric(location, event.metric.member.name, event.time)

    def __str__(self):
        out = ""
        for interval in self._address_spaces:
            out += "{}\n\n".format(interval.data)
        return out


# TODO Factory?
def make_mmap_space(attributes):
    if attributes:
        ms = AddressSpace()
        for attribute in attributes:
            if attribute.name == MMAP_SIZE_TAG:
                ms.Size = attributes[attribute]
            elif attribute.name == MMAP_ADDRESS_TAG:
                ms.Address = attributes[attribute]
            elif attribute.name == MMAP_SOURCE_TAG:
                ms.Source = attributes[attribute]
        return ms if ms.Size != -1 and ms.Address != -1 else None
    return None


def make_scorep_space(trace):
    space = AddressSpace()
    if trace:
        for prop in trace.definitions.location_properties:
            if prop.name == SCOREP_MEMORY_ADDRESS:
                space.Address = int(prop.value)
            elif prop.name == SCOREP_MEMORY_SIZE:
                space.Size = int(prop.value)
        space.Source = "Score-P"
        return space if space.Address != -1 and space.Size != -1 else None
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", help="Path to trace file i.e. trace.otf2", type=str)
    args = parser.parse_args()

    with otf2.reader.open(args.trace) as trace_reader:
        with otf2.writer.open("rewrite", definitions=trace_reader.definitions) as trace_writer:
            mmio_stats = MemoryMappedIo()
            for location, event in trace_reader.events:
                event_writer = trace_writer.event_writer_from_location(location)
                event_writer(event)
                mapped_space = make_mmap_space(event.attributes)
                if mapped_space:
                    mmio_stats.add_mapped_space(location, mapped_space, event.time, trace_writer)
                if isinstance(event, otf2.events.Metric) and AccessType.contains(event.metric.member.name):
                    mmio_stats.add_access(event, location)
