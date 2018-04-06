#! /usr/bin/env python3
import sys
import os.path
import otf2
import collections
import math
import json
import argparse
from intervaltree import Interval, IntervalTree
from otf2.events import IoOperationBegin

PARADIGM_IDS = {"POSIX", "ISOC"}

class ClockConverter:
    def __init__(self, clock_properties: otf2.definitions.ClockProperties):
        self.properties = clock_properties

    def to_usec(self, ticks: int) -> float:
        return float(ticks / (self.properties.timer_resolution / 1000000))

    def to_sec(self, ticks: int) -> float:
        return float(ticks / self.properties.timer_resolution)

    def to_ticks(self, secs: float) -> int:
        return secs * self.properties.timer_resolution

class IoStat:
    def __init__(self):
        self.read_count = 0
        self.write_count = 0

    def incReadCount(self) -> None:
        self.read_count += 1

    def incWriteCount(self) -> None:
        self.write_count += 1

    def __str__(self) -> str:
        return "read_count: {}, write_count: {}".format(self.read_count, self.write_count)

def is_posix(identification: str) -> bool:
    return identification in PARADIGM_IDS

def print_tree(tree: IntervalTree) -> None:
    for i in sorted(tree):
        print("{}:{} -> {}".format(i.begin, i.end, i.data))

def get_interval(timestamp: int, tree: IntervalTree) -> Interval:
    result = sorted(tree.search(timestamp, strict=True))
    assert(len(result) == 1)
    return result[0]

def generate_intervals(trace: otf2.reader.Reader, clock: ClockConverter, length: int) -> tuple:
    start = clock.properties.global_offset
    end = start + clock.properties.trace_length
    for loc_group in trace.definitions.location_groups:
        if loc_group.location_group_type == otf2.enums.LocationGroupType.PROCESS:
            yield (loc_group.name, IntervalTree(Interval(i, i + length, IoStat()) for i in range(start, end, length)))

def parse_proc_stats(io_stats: dict) -> dict:
    for proc in io_stats:
        proc_stats = {"read": [], "write" : []}
        for interval in sorted(io_stats[proc]):
            proc_stats["read"].append(interval.data.read_count)
            proc_stats["write"].append(interval.data.write_count)
        yield (proc, proc_stats)

def store_stats(io_stats: dict, path: str) -> None:
    out = {proc: stats for proc, stats in parse_proc_stats(io_stats)}
    with open("{}/io_stats.json".format(path), 'w') as file:
        json.dump(out, file)

def get_io_operation_count(trace_file: str, interval_length: float = None, step_count: int = None) -> dict:
    with otf2.reader.open(trace_file) as trace:
        clock = ClockConverter(trace.definitions.clock_properties)
        if interval_length:
            length = int(clock.to_ticks(interval_length))
            step_count = int(clock.properties.trace_length / length)
        else:
            length = int(clock.properties.trace_length / step_count)

        print("Created {} intervals of length {} secs".format(step_count, clock.to_sec(length)))
        io_stats = {proc: interval for (proc, interval) in generate_intervals(trace, clock, length)}

        for location, event in trace.events:
            if isinstance(event, IoOperationBegin) and is_posix(event.handle.io_paradigm.identification):
                if event.mode == otf2.enums.IoOperationMode.WRITE:
                    tree = io_stats[location.group.name]
                    get_interval(event.time, tree).data.incWriteCount()
                if event.mode == otf2.enums.IoOperationMode.READ:
                    tree = io_stats[location.group.name]
                    get_interval(event.time, tree).data.incReadCount()

        return io_stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", help="Path to trace file i.e. trace.otf2", type=str)
    parser.add_argument("output", help="Path to output directory", type=str)
    parser.add_argument("--num_intervals", help="Number of intervals in which the trace will be cutted.", type=int, default=10)
    parser.add_argument("--interval_length", help="Specifies the length of an interval in seconds(float).", type=float)
    args = parser.parse_args()

    if not os.path.exists(args.output):
        sys.exit("Given path does not exist.")
    io_stats = get_io_operation_count(args.trace, args.interval_length, args.num_intervals)
    store_stats(io_stats, args.output)
