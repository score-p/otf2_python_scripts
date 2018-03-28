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

def process_interval(trace: otf2.reader.Reader, step_count: int) -> tuple:
    for loc_group in trace.definitions.location_groups:
        if loc_group.location_group_type == otf2.enums.LocationGroupType.PROCESS:
            yield (loc_group.name, IntervalTree(Interval(freq * i, (freq * i) + freq, IoStat()) for i in range(0, step_count)))

if __name__ == '__main__':
    file = "/home/cherold/scorep/tests/isoc/scorep-20171220_1411_54178004785460/traces.otf2"
    with otf2.reader.open(file) as trace:
        clock = ClockConverter(trace.definitions.clock_properties)
        step_count = 200
        freq = int(clock.clock_properties.trace_length / step_count)
        io_stats = {proc: interval for (proc, interval) in process_interval(trace, step_count)}
        for location, event in trace.events:
            if isinstance(event, IoOperationBegin) and is_posix(event.handle.io_paradigm.identification):
                if event.mode == otf2.enums.IoOperationMode.WRITE:
                    tree = io_stats[location.group.name]
                    get_interval(event.time - clock.clock_properties.global_offset, tree).data.incWriteCount()
                if event.mode == otf2.enums.IoOperationMode.READ:
                    tree = io_stats[location.group.name]
                    get_interval(event.time - clock.clock_properties.global_offset, tree).data.incWriteCount()

        for proc in io_stats:
            print_tree(io_stats[proc])
