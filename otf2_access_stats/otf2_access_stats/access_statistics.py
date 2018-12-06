#! /usr/bin/env python3

import sys
import os.path
from collections import defaultdict
import argparse
from flask import Flask, jsonify, render_template, request
import argparse
import otf2
from otf2.enums import LocationType

from spacecollection import AddressSpace, AccessType, count_loads
from spacestatistics import MemoryAccessStatistics


COLORS = ["#0000FF",
          "#00FF00",
          "#FFFF00",
          "#FF8000",
          "#FF0000",
          "#7F00FF",
          "#00FF00",
          "#808080"]

app = Flask(__name__)

app.config['location_mapping'] = dict()
app.config['location_mapping']['ALL'] = None


def process_trace(trace):
    stats = MemoryAccessStatistics()
    with otf2.reader.open(trace) as trace_reader:
        for location, event in trace_reader.events:
            if location.type == LocationType.CPU_THREAD and not location.name in app.config['location_mapping']:
                app.config['location_mapping'][location.name] = location
            space = AddressSpace(attributes=event.attributes)
            if space.initialized():
                stats.add_mapped_space(space)
            if isinstance(event, otf2.events.Metric) and AccessType.contains(event.metric.member.name):
                stats.add_access(event, location)
    return stats


def get_space_colors():
    space_colors = {}
    for i, space_name in enumerate(sorted(app.config['space_stats'].keys()), 0):
        space_colors[space_name] = COLORS[i]
    return space_colors


@app.route('/_get_space_util_for_thread')
def get_space_util_for_thread():
    selected_thread = request.args.get('selected_thread', 0, type=str)
    print("Selected {}".format(selected_thread))
    location = app.config['location_mapping'][selected_thread]

    stats_per_source = app.config['memory_access_stats'].resource_utilization(location)
    colors_dict = get_space_colors()
    colors = [colors_dict[s] for s in stats_per_source]

    print(stats_per_source)
    print(colors)
    return jsonify(labels=list(stats_per_source.keys()),
                    colors=colors,
                    data=list(stats_per_source.values()))


@app.route('/_get_thread_stats_per_space')
def get_thread_stats_per_space():
    selected_space = request.args.get('selected_space', 0, type=str)
    load_distribution = defaultdict(int)
    store_distribution = defaultdict(int)
    for space in app.config['space_stats'][selected_space]:
        for loc, seq in space.get_all_accesses():
            loads = count_loads(seq)
            stores = len(seq) - loads
            load_distribution["Sum"] += loads
            store_distribution["Sum"] += stores
            load_distribution[loc.name] += loads
            store_distribution[loc.name] += stores


    return jsonify(labels=list(load_distribution.keys()),
                   loads=list(load_distribution.values()),
                   stores=list(store_distribution.values()))


@app.route('/_get_stats_per_space')
def get_stats_per_space():
    selected_space = request.args.get('selected_space', 0, type=str)
    nloads = 0
    nstores = 0
    for space in app.config['space_stats'][selected_space]:
        (nl, ns) = space.count_access_types()
        nloads += nl
        nstores += ns
    return jsonify(nloads=nloads,nstores=nstores)


@app.context_processor
def utility_processor():

    def get_space_utilization():
        space_utilization = defaultdict(int)
        for key, space_list in app.config['space_stats'].items():
            for space in space_list:
                space_utilization[key] = sum([len(access_seq) for _, access_seq in space.get_all_accesses()])
        return space_utilization

    def get_space_lengths():
        return [sum([space.Size for space in spaces]) for spaces in app.config['space_stats'].values()]

    return dict(get_space_utilization=get_space_utilization,
                get_space_colors=get_space_colors,
                get_space_lengths=get_space_lengths)


@app.route('/')
def index():
    app.config['memory_access_stats'] = process_trace(app.config['trace'])
    app.config['space_stats'] = app.config['memory_access_stats'].get_space_stats()
    return render_template("index.html")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", help="Path to trace file i.e. trace.otf2", type=str)
    args = parser.parse_args()
    if args.trace:
        app.config['trace'] = args.trace
        app.run()
