#! /usr/bin/env python3
import sys
import os.path
import time
import otf2
from otf2.enums import Type

MMAP_SIZE_TAG = "mappedSize"
MMAP_ADDRESS_TAG = "startAddress"
MMAP_SOURCE_TAG = "mappedSource"
LOAD_METRIC="MemoryAccess:load"
STORE_METRIC="MemoryAccess:store"
SCOREP_MEMORY_ADDRESS = "scorep:memoryaddress:begin"
SCOREP_MEMORY_SIZE = "scorep:memoryaddress:len"
TIMER_GRANULARITY = 1000000

def t():
    return int(round(time.time() * TIMER_GRANULARITY))

if __name__ == "__main__":
    with otf2.writer.open("test_trace_01", timer_resolution=TIMER_GRANULARITY) as trace:
        root_node = trace.definitions.system_tree_node("root node")
        system_tree_node = trace.definitions.system_tree_node("myHost", parent=root_node)
        location_group = trace.definitions.location_group("Master Process", system_tree_parent=system_tree_node)
        writer0 = trace.event_writer("Thread0", group=location_group)
        writer1 = trace.event_writer("Thread1", group=location_group)

        address_attr = trace.definitions.attribute(name=MMAP_ADDRESS_TAG, description="Address attribute", type=Type.UINT64)
        size_attr = trace.definitions.attribute(name=MMAP_SIZE_TAG, description="Size attribute", type=Type.UINT64)
        source_attr = trace.definitions.attribute(name=MMAP_SOURCE_TAG, description="Source attribute", type=Type.STRING)

        main_func = trace.definitions.region("main")
        pmalloc_func = trace.definitions.region("pmmalloc")
        malloc_func = trace.definitions.region("mmalloc")
        load_metric = trace.definitions.metric(LOAD_METRIC, unit="address", value_type=Type.UINT64)
        store_metric = trace.definitions.metric(STORE_METRIC, unit="address", value_type=Type.UINT64)

        writer0.enter(t(), main_func)
        writer1.enter(t(), main_func)

        writer0.enter(t(), pmalloc_func, attributes={address_attr: 100, size_attr: 32, source_attr: "NVRAM"})
        writer0.leave(t(), pmalloc_func)
        writer0.enter(t(), malloc_func, attributes={address_attr: 200, size_attr: 32, source_attr: "HEAP"})
        writer0.leave(t(), malloc_func)

        writer0.metric(t(), load_metric, 100)
        writer0.metric(t(), load_metric, 102)
        writer0.metric(t(), store_metric, 101)

        writer0.metric(t(), load_metric, 200)
        writer0.metric(t(), store_metric, 201)
        writer0.metric(t(), store_metric, 202)

        writer0.metric(t(), load_metric, 300)
        writer0.metric(t(), store_metric, 304)
        writer0.metric(t(), store_metric, 305)

        writer1.metric(t(), load_metric, 110)
        writer1.metric(t(), store_metric, 111)

        writer1.metric(t(), load_metric, 210)
        writer1.metric(t(), store_metric, 211)

        writer1.metric(t(), load_metric, 1300)
        writer1.metric(t(), store_metric,1304)
        writer1.metric(t(), store_metric, 1305)
        writer0.leave(t(), main_func)
        writer1.leave(t(), main_func)