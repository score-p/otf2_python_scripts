#! /usr/bin/env python3

import sys
import os.path
from collections import defaultdict
import argparse
import otf2

from otf2_access_stats.spacecollection import AddressSpace, AccessType
from otf2_access_stats.spacestatistics import MemoryAccessStatistics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", help="Path to trace file i.e. trace.otf2", type=str)
    parser.add_argument('--counters', action="store_true", help='Creates metrics which counts the number of accesses per source.')
    parser.add_argument('--accesses', action="store_true", help='Creates metrics that contains the virtual address accessed source.')
    args = parser.parse_args()

    if args.accesses or args.counters:
        with otf2.reader.open(args.trace) as trace_reader:
            stats = MemoryAccessStatistics(trace_reader.timer_resolution)
            trace_writer = otf2.writer.Writer("rewrite", definitions=trace_reader.definitions)
            for location, event in trace_reader.events:
                event_writer = trace_writer.event_writer_from_location(location)
                event_writer(event)
                space = AddressSpace(attributes=event.attributes)
                if space.initialized():
                    stats.add_mapped_space(space)
                if isinstance(event, otf2.events.Metric) and AccessType.contains(event.metric.member.name):
            if args.accesses:
                stats.create_access_metrics(trace_writer)
            if args.counters:
                stats.create_counter_metrics(trace_writer)
