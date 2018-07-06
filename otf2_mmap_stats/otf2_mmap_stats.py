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

class MappedSpace:

    def __init__(self):
        self.Size = -1
        self.Source = ""
        self.Address = -1

    def __str__(self):
        return "[{}, {}] = Size: {}, Source {}, {}".format( self.Address,
                                                            self.Address + self.Size,
                                                            self.Size,
                                                            self.Source )

class AccessType (Enum):

    LOAD = auto()
    STORE = auto()

    @classmethod
    def get_access_type(cls, str):
        if str == "MemoryAccess:load":
            return cls.LOAD
        elif str ==  "MemoryAccess:store":
            return cls.STORE
        else:
            exit("Invalid type given")

    @classmethod
    def contains(cls, str):
        if str == "MemoryAccess:load":
            return True
        elif str ==  "MemoryAccess:store":
            return True
        else:
            return False


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


class SpaceStatistics:

    def __init__(self, space, trace, timestamp):
        self.Space = space
        self.LoadMetric = dict()
        self.StoreMetric = dict()
        for loc in trace.definitions.locations:
            if loc.type == otf2.LocationType.CPU_THREAD:
                event_writer = trace.event_writer_from_location(loc)
                self.LoadMetric[loc] = MyMetric(trace, event_writer, "{}:Load".format(self.Space.Source), timestamp)
                self.StoreMetric[loc] = MyMetric(trace, event_writer, "{}:Store".format(self.Space.Source), timestamp)

    def inc_metric(self, location, metric_name, timestamp):
        if AccessType.get_access_type(metric_name) == AccessType.LOAD:
            self.LoadMetric[location].inc(timestamp)
        elif AccessType.get_access_type(metric_name) == AccessType.STORE:
            self.StoreMetric[location].inc(timestamp)

    def __str__(self):
        return "{} {} {}".format(self.Space, self.LoadMetric, self.StoreMetric)

class MemoryMappedIoStats:

    def __init__(self):
        # Interval -> SpaceStatistics
        self.address_spaces = IntervalTree()
        self.number_of_accesses = 0

    def add_mapped_space(self, location, space, timestamp, trace):
        if space:
            self.address_spaces.addi(space.Address,
                                     space.Address + space.Size,
                                     SpaceStatistics(space,
                                                     trace,
                                                     timestamp))

    def add_access(self, event, location):
        self.number_of_accesses += 1
        intervals = self.address_spaces[int(event.value)]
        assert(len(intervals) < 2)
        if len(intervals) == 1:
            space_stats = intervals.pop().data
            space_stats.inc_metric(location, event.metric.member.name, event.time)

    def __str__(self):
        out = ""
        for interval in self.address_spaces:
            out += "{}\n\n".format(interval.data)
        return out

# TODO Factory?
def make_mmap_space(attributes):
    if attributes:
        ms = MappedSpace()
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
    space = MappedSpace()
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
            mmio_stats = MemoryMappedIoStats()
            for location, event in trace_reader.events:
                event_writer = trace_writer.event_writer_from_location(location)
                event_writer(event)
                mapped_space = make_mmap_space(event.attributes)
                if mapped_space:
                    mmio_stats.add_mapped_space(location, mapped_space, event.time, trace_writer)
                if isinstance(event, otf2.events.Metric) and AccessType.contains(event.metric.member.name):
                    mmio_stats.add_access(event, location)
