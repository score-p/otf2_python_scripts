#! /usr/bin/env python3
import sys
import os.path
from collections import defaultdict
from os import listdir
import argparse

import otf2

import tracefile as tf

from otf2_access_stats.spacecollection import AddressSpace, AccessType
from otf2_access_stats.spacestatistics import MemoryAccessStatistics


def get_traces(path, suffix=".bin"):
    for filename in listdir(path):
        if filename.endswith(suffix):
            yield "{}/{}".format(path,filename)


def get_space_statistic(otf2_trace):
    with otf2.reader.open(otf2_trace) as trace_reader:
        statistic = MemoryAccessStatistics(trace_reader.timer_resolution)
        for location, event in trace_reader.events:
            space = AddressSpace(attributes=event.attributes)
            if space.initialized():
                statistic.add_mapped_space(space)
        return statistic


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("otf2trace", help="Path to the OTF2 trace file i.e. trace.otf2", type=str)
    parser.add_argument("accesstrace", help="Path to the access trace file i.e. trace.123.bin", type=str)
    args = parser.parse_args()

    space_stat = get_space_statistic(args.otf2trace)

    for trace in get_traces(args.accesstrace):
        with tf.TraceFile(trace, tf.TraceFileMode.READ) as th:
            read_buffer, md = th.read()
            space_stat.add_all_accesses(read_buffer, md.thread_id)

    level_stats = space_stat.memory_level_usage()
    for source, levels in level_stats.items():
        print("{}".format(source))
        for level, counter in levels.items():
            print("\t{}".format(level))
            print("\t{}\n".format(counter))
