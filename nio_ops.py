import sys
import otf2
import collections
import math
import argparse
from intervaltree import Interval, IntervalTree
from otf2.events import IoOperationBegin

PARADIGM_IDS = {"POSIX", "ISOC"}

class ClockConverter:
    def __init__(self, clock_properties: otf2.definitions.ClockProperties):
        self.clock_properties = clock_properties
        self.uninitialized_warning = "ClockConverter was not initialized, as a consequence time output values are measured in ticks."

    def to_usec(self, ticks: int) -> float:
        return float(ticks / (self.clock_properties.timer_resolution / 1000000))

    def to_sec(self, ticks: int) -> float:
        return float(ticks / self.clock_properties.timer_resolution)

    def to_ticks(self, secs: float) -> int:
        assert(secs <= self.to_sec(self.clock_properties.trace_length))
        return secs * self.clock_properties.timer_resolution


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

def process_interval(trace: otf2.reader.Reader, clock: ClockConverter, length: int) -> tuple:
    start = clock.clock_properties.global_offset
    end = start + clock.clock_properties.trace_length
    # TODO check computed range
    for loc_group in trace.definitions.location_groups:
        if loc_group.location_group_type == otf2.enums.LocationGroupType.PROCESS:
            yield (loc_group.name, IntervalTree(Interval(i, i + length, IoStat()) for i in range(start, end, length)))

def io_operation_count() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", help="Path to trace file i.e. trace.otf2", type=str)
    parser.add_argument("--num_intervals", help="Number of intervals in which the trace will be cutted.", type=int, default=10)
    parser.add_argument("--interval_length", help="Specifies the length of an interval in seconds(float).", type=float)
    args = parser.parse_args()
    step_count = args.num_intervals

    with otf2.reader.open(args.trace) as trace:
        clock = ClockConverter(trace.definitions.clock_properties)
        if args.interval_length:
            length = int(clock.to_ticks(args.interval_length))
            step_count = int(clock.clock_properties.trace_length / length)
        else:
            length = int(clock.clock_properties.trace_length / step_count)

        print("Create {} intervals of length {} secs".format(step_count, clock.to_sec(length)))
        io_stats = {proc: interval for (proc, interval) in process_interval(trace, clock, length)}

        for location, event in trace.events:
            if isinstance(event, IoOperationBegin) and is_posix(event.handle.io_paradigm.identification):
                if event.mode == otf2.enums.IoOperationMode.WRITE:
                    tree = io_stats[location.group.name]
                    get_interval(event.time, tree).data.incWriteCount()
                if event.mode == otf2.enums.IoOperationMode.READ:
                    tree = io_stats[location.group.name]
                    get_interval(event.time, tree).data.incReadCount()

        for proc in io_stats:
            print_tree(io_stats[proc])
