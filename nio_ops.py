#!/usr/bin/env python3

import sys
import otf2
import collections
import math
from intervaltree import Interval, IntervalTree
from otf2.events import *

class ClockConverter:
    clock_properties = None

    def __init__(self, clock_properties: otf2.definitions.ClockProperties):
        self.clock_properties = clock_properties
        print("{}".format(clock_properties))
        self.uninitialized_warning = "ClockConverter was not initialized, as a consequence time output values are measured in ticks."

    def to_usec(self, ticks: int) -> float:
        # assert(ticks > self.clock_properties.global_offset)
        if self.clock_properties is not None:
            return float(ticks / (self.clock_properties.timer_resolution / 1000000))
        else:
            return 0.0

    def to_sec(self, ticks: int) -> float:
        # assert(ticks > self.clock_properties.global_offset)
        if self.clock_properties is not None:
            return float(ticks / self.clock_properties.timer_resolution)
        else:
            return 0.0

class IoStat:
    def __init__(self):
        self.nread = 0
        self.nwrite = 0

def is_posix(identification: str) -> bool:
    return identification == "POSIX" or identification == "ISOC"

def print_tree(tree: IntervalTree) -> None:
    for i in sorted(tree):
        print("{}:{}, {}, {}".format(i.begin, i.end, i.data.nread, i.data.nwrite))

file = "/home/cherold/scorep/tests/isoc/scorep-20171220_1411_54178004785460/traces.otf2"

freq = 50000

number_of_reads = 0
number_of_writes = 0

clock = None

with otf2.reader.open(file) as trace:
    clock = ClockConverter(trace.definitions.clock_properties)
    nsteps = 200
    freq = int(clock.clock_properties.trace_length / nsteps)
    tree = IntervalTree(Interval(freq * i, (freq * i) + freq, IoStat()) for i in range(0, nsteps))
    for location, event in trace.events:
        if isinstance(event, IoOperationBegin) and is_posix(event.handle.io_paradigm.identification):
            if event.mode == otf2.enums.IoOperationMode.WRITE:
                number_of_writes += 1
            if event.mode == otf2.enums.IoOperationMode.READ:
                number_of_reads += 1
        event_time = event.time - clock.clock_properties.global_offset
        print(event_time)
        for i in tree.search(event_time, strict=True):
            i.data.nread = number_of_reads
            i.data.nwrite = number_of_writes
    print_tree(tree)
