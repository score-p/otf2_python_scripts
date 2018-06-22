#! /usr/bin/env python3
import sys
import os.path
import otf2
import collections
import math
import json
import argparse
from intervaltree import Interval, IntervalTree
from otf2.events import *

MEMACCESS_COUNTER = "Memory access"
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
        self.Accesses = 0
        self.Metric = None

    def __str__(self):
        return "[{}, {}] = Size: {}, Source {}, {}".format(self.Address,
                                                           self.Address + self.Size,
                                                           self.Size,
                                                           self.Source,
                                                           self.Accesses)


class AccessStatistics:
    def __init__(self):
        self.address_spaces = IntervalTree()
        self.number_of_accesses = 0

    def add_mapped_space(self, space):
        if space:
            self.address_spaces.addi(space.Address,
                                     space.Address + space.Size,
                                     space )

    def add_access(self, address):
        self.number_of_accesses += 1
        i = self.address_spaces[address]
        assert(len(i) < 2)
        if len(i) == 1:
            space = i.pop().data
            space.Accesses += 1
            return space
        return None

    def __str__(self):
        out = ""
        for interval in self.address_spaces:
            data = interval.data
            out += "{}: {}\n".format(data.Source, data.Accesses)
        out += "Number of accesses: {}\n".format(self.number_of_accesses)
        return out


def init_mmap_space(attributes):
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

def init_scorep_space(trace):
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
    access_stats = AccessStatistics()

    with otf2.reader.open(args.trace) as trace_reader:
        with otf2.writer.open("rewrite", definitions=trace_reader.definitions) as trace_writer:
            scorep_space = init_scorep_space(trace_reader)
            access_stats.add_mapped_space(scorep_space)
            for location, event in trace_reader.events:
                event_writer = trace_writer.event_writer_from_location(location)
                event_writer(event)
                mapped_space = init_mmap_space(event.attributes)
                if mapped_space:
                    mapped_space.Metric = trace_writer.definitions.metric(mapped_space.Source, unit="Number of Accesses")
                    event_writer.metric(event.time, mapped_space.Metric, 0)
                    access_stats.add_mapped_space(mapped_space)
                if isinstance(event, Metric) and event.metric.member.name == MEMACCESS_COUNTER:
                    space = access_stats.add_access(int(event.value))
                    if space:
                        event_writer.metric(event.time, space.Metric, space.Accesses)
    print(access_stats)