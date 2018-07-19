#!/usr/bin/env python

# To make divisions convert operands to float
from __future__ import division

import sys

import otf2
import _otf2
import os
import shutil
from functools import reduce
import argparse

def gather_traces(trace_folder):
    """Get all traces from each subdirectory of trace_folder"""
    sub_folders = next(os.walk(trace_folder))[1]
    trace_files = [os.path.join(trace_folder, curDir, "traces.otf2") for curDir in sub_folders]
    return list(filter(lambda curDir: os.path.exists(curDir), trace_files))

# Workaround for OTF2 bug: Missing method in _RefRegistry.
# Calling Code: _set_field(self, field, value)
#                   old_key = self._key
#                   value = field.check_value(value)
#                   setattr(self, field.private_name, value)
#                   self._registry._update(old_key, self)
#
def _update(self, old_key, obj):
    del self._elements_by_key[old_key]
    self._elements_by_key[obj._key] = obj

otf2.registry._RefRegistry._update = _update

def getSortedEvents(trace_readers, fixup_time):
    curEvents = [None for _ in trace_readers]
    iters = [reader.events.__iter__() for reader in trace_readers]
    while True:
        minEl = None
        for i, event in enumerate(curEvents):
            if event is None:
                event = next(iters[i], None)
                if event is not None:
                    event[1].time = fixup_time(event[1].time, trace_readers[i])
                    curEvents[i] = event
            if event is not None and (minEl is None or minEl[1][1].time > event[1].time):
                minEl = (i, event)
        if minEl is None:
            break
        curEvents[minEl[0]] = None
        yield minEl

def find_by_name(name, iterable):
    return next( (x for x in iterable if x.name == name), None)

def get_properties(obj):
    """Get all properties from the object that are public (don't start with underscore)"""
    for property in dir(obj):
        # undefined_ref and type_id should have been private too
        if property[0] != '_' and property != 'undefined_ref' and property != 'type_id':
            value = getattr(obj, property)
            # Don't return methods
            if not callable(value):
                yield (property, value)

def is_trivial_type(val):
    """Return true if this is a trivial (non-reference) type"""
    if val is None:
        return True
    if isinstance(val, _otf2.EnumBase):
        return True
    trivial_types = (str, int, float)
    if sys.version_info < (3,):
        trivial_types = trivial_types + (long,)
    return isinstance(val, trivial_types)

def register_translated_obj(orig_obj, new_obj, dest):
    try:
        translated_objs = dest._translated_objs
    except:
        dest._translated_objs = {}
        translated_objs = dest._translated_objs
    # Store orig_obj to keep it alive. Else the id is not unique!
    translated_objs[id(orig_obj)] = (orig_obj, new_obj)

def get_translated_obj(orig_obj, dest):
    try:
        return dest._translated_objs[id(orig_obj)][1]
    except:
        return None

def clone_obj(obj, dest, do_register = True):
    assert(type(do_register) is bool)
    new_obj = get_translated_obj(obj, dest)
    if new_obj is not None:
        return new_obj
    if isinstance(obj, otf2.events._Event):
        ctor = type(obj)
    else:
        try:
            registry = dest.definitions._registry_for_type(type(obj))
            ctor = registry.create
        except:
            raise Exception("Unhandled type found: {}: {}".format(type(obj), obj))

    new_obj = {}
    for property, value in get_properties(obj):
        if not is_trivial_type(value):
            value = clone_obj(value, dest)
        new_obj[property] = value

    new_obj = ctor(**new_obj)
    if do_register:
        register_translated_obj(obj, new_obj, dest)
    return new_obj

def clone_event(obj, dest):
    return clone_obj(obj, dest, False)

def clone_or_register(obj, new_obj, dest):
    """If new_obj is valid it is registered for obj, else obj is cloned"""
    if new_obj:
        register_translated_obj(obj, new_obj, dest)
        return new_obj
    else:
        return clone_obj(obj, dest)

def event_name(event):
    try:
        return event.region.name
    except:
        pass
    try:
        return event.parameter.name
    except:
        pass
    return str(event)

class Period(object):
    def __init__(self, start_time, start_offset, end_time, end_offset):
        self.begin = start_time
        self.end = end_time
        self.offset = start_offset
        self.diff_offset = end_offset - self.offset
        self.slope = self.diff_offset / (self.end - self.begin)

    def interpolate(self, time):
        # Avoid precision loss by using float only as long as required
        interpolated_offset = round(self.slope * (time - self.begin))
        return time + self.offset + int(interpolated_offset)

class LocationEventWriter(object):
    def __init__(self, archive_writer, location):
        self.location = location
        self._writer = archive_writer.event_writer_from_location(location)
        self.min_time = self.max_time = None
        self._periods = None
        self.clock_offsets = []

    def update_timestamps(self, time):
        if self.min_time is None:
            self.min_time = self.max_time = time
        else:
            self.min_time = min(self.min_time, time)
            self.max_time = max(self.max_time, time)

    def add_clock_offset(self, timestamp, offset):
        if self.clock_offsets:
            if self.clock_offsets[-1][0] >= timestamp:
                raise Exception("Multiple timer synchronization at the same time")
        self.clock_offsets.append((timestamp, offset))

    def interpolate_time(self, timestamp):
        if not self._periods:
            return timestamp
        cur_period = self._periods[0]
        for period in self._periods:
            if timestamp <= period.end:
                break
            cur_period = period
        return cur_period.interpolate(timestamp)

    def update_archive_time(self, archive):
        """Updates the min/max timestamps in archive"""
        if self.min_time is None:
            return
        archive._update_timestamps(self.interpolate_time(self.min_time))
        archive._update_timestamps(self.interpolate_time(self.max_time))

    def write(self, event):
        self.update_timestamps(event.time)
        self._writer(event)

    def get_min_offset(self):
        """Get the minimum offset in clock_offsets"""
        return reduce(min, [o[1] for o in self.clock_offsets]) if self.clock_offsets else None

    def finalize_periods(self, offset):
        """Combine each pair in clock_offsets into 1 period

           offset: Subtracted from each offset to shift them into a specific range
           Note: OTF2 does not reuse the 2nd part of the pair for the next period. Use once and discard"""
        assert(self._periods is None)
        self._periods = []
        last = None
        for clock_offset in self.clock_offsets:
            if last is None:
                last = clock_offset
            else:
                start_offset = last[1] - offset
                end_offset = clock_offset[1] - offset
                self._periods.append(Period(last[0], start_offset, clock_offset[0], end_offset))
                last = None

    def write_definitions(self, archive):
        self.update_archive_time(archive)
        for period in self._periods:
            # Write start and end clock offset
            _otf2.DefWriter_WriteClockOffset(self._writer._def_handle, period.begin, period.offset, 0.)
            _otf2.DefWriter_WriteClockOffset(self._writer._def_handle, period.end, period.offset + period.diff_offset, 0.)


class EventWriter(object):
    def __init__(self, writer):
        self._writer = writer
        self.loc_writers = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            return False
        self.close()
        return True

    def _get_writer(self, location):
        loc_id = id(location)
        if loc_id not in self.loc_writers:
            loc_writer = LocationEventWriter(self._writer, location)
            self.loc_writers[loc_id] = loc_writer
        else:
            loc_writer = self.loc_writers[loc_id]
        return loc_writer

    def write(self, location, event):
        self._get_writer(location).write(event)

    def clock_offset(self, location, timestamp, offset):
        self._get_writer(location).add_clock_offset(timestamp, offset)

    def close(self):
        self._writer._first_timestamp = self._writer._last_timestamp = None
        # Change offset to be minimal. This avoids precision issues in OTF2
        min_offset = None
        for offset in [l.get_min_offset() for l in self.loc_writers.values()]:
            if offset is not None and (min_offset is None or offset < min_offset):
                min_offset = offset
        for loc_writer in self.loc_writers.values():
            loc_writer.finalize_periods(min_offset)
            loc_writer.write_definitions(self._writer)

class TimeTranslater(object):
    def __init__(self, target_resolution, target_offset = 0):
        self.resolution = target_resolution
        self.offset = target_offset

    def translate(self, time, reader):
        clock_props = reader.definitions.clock_properties
        time = self.translate_resolution(time - clock_props.global_offset, clock_props.timer_resolution)
        return time + self.offset

    def translate_resolution(self, time, timer_resolution):
        # Translate the timestamp from the timer resolution to the current resolution
        time *= self.resolution / timer_resolution
        return int(round(time))

def prettify_names(trace_readers, trace_files, output_trace):
    """Change the names of various definitions to some sensible values"""
    out_defs = output_trace.definitions
    for i, reader in enumerate(trace_readers):
        folder_name = os.path.basename(os.path.dirname(trace_files[i]))
        # Combine same nodes (by name)
        for node in reader.definitions.system_tree_nodes:
            new_node = find_by_name(node.name, out_defs.system_tree_nodes)
            clone_or_register(node, new_node, output_trace)
        # Combine regions (note: Assumes regions with same name are the same, could also check role, source_file...)
        for region in reader.definitions.regions:
            # Skip meta region used only by this script
            if region.name == "__init":
                continue
            new_reg = find_by_name(region.name, out_defs.regions)
            clone_or_register(region, new_reg, output_trace)

        # Prettify location groups
        group_name = "Process " + folder_name
        for group in reader.definitions.location_groups:
            # Sanity checks to avoid unintended renames
            if group.location_group_type != otf2.LocationGroupType.PROCESS:
                continue
            if group.name != "Process":
                continue
            outgroup = clone_obj(group, output_trace)
            outgroup.name = group_name

def combine_traces(trace_files, out_folder):
    """Combine all traces into one and write it into out_folder"""
    if not trace_files:
      raise Exception("No traces found")
    trace_readers = []
    try:
        for traceFile in trace_files:
            print("Reading {}".format(traceFile))
            trace_readers.append(otf2.reader.Reader(traceFile))
        timer_resolution = trace_readers[0].timer_resolution
        with otf2.writer.open(out_folder, timer_resolution=timer_resolution) as write_trace:
            prettify_names(trace_readers, trace_files, write_trace)

            time_translater = TimeTranslater(write_trace.definitions.clock_properties.timer_resolution)

            with EventWriter(write_trace) as writer:
                writer.time_translater = time_translater
                first_sync_point = None
                for i, (loc, event) in getSortedEvents(trace_readers, time_translater.translate):
                    outloc = clone_obj(loc, write_trace)
                    if isinstance(event, otf2.events.ParameterInt) and event.parameter.name == "__syncTime":
                        # Translate the epoch to the first timestamp recorded.
                        # This helps keeping the values low which reduces precision errors during translation
                        if first_sync_point is None:
                            first_sync_point = (event.time, event.value)
                        elapsed_real_time = event.value - first_sync_point[1]
                        # From definition of offset: timestamp + offset = realtime
                        # real time is in nanoseconds
                        offset = time_translater.translate_resolution(elapsed_real_time, 1e9) - event.time
                        writer.clock_offset(outloc, event.time, offset)
                        # Don't write sync params
                        continue
                    if isinstance(event, (otf2.events.Enter, otf2.events.Leave)) and event.region.name == "__init":
                        continue
                    event = clone_event(event, write_trace)
                    writer.write(outloc, event)
                for loc_writer in writer.loc_writers.values():
                    if len(loc_writer.clock_offsets) == 1:
                        offset = loc_writer.clock_offsets[0][1]
                        last_time = loc_writer.max_time
                        # Offset is assumed to be constant
                        loc_writer.add_clock_offset(last_time, offset)
    finally:
        for reader in trace_readers:
            reader.close()

def main():
    parser = argparse.ArgumentParser(description="Combine multiple generated OTF2 traces into one")
    parser.add_argument(
        "-i", "--input",
        type=str, required=True,
        help="The folder containing all traces (e.g. .../logs/scorep)",
    )
    parser.add_argument(
        "-o", "--output",
        type=str, required=True,
        help="The output folder receiving the trace. Should be empty",
    )
    parser.add_argument(
        "-c", "--clean",
        action = "store_true",
        help="Clean (delete) the output folder if it exists",
    )
    args = parser.parse_args()

    out_folder = args.output
    if os.path.exists(out_folder) and args.clean:
        shutil.rmtree(out_folder)

    combine_traces(gather_traces(args.input), out_folder)

if __name__ == '__main__':
    main()

