#! /usr/bin/env python3

import sys
import os.path
from collections import defaultdict
import argparse
from flask import Flask, jsonify, render_template, request
import argparse
import otf2

from spacecollection import AddressSpace, AccessType
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


def process_trace(trace):
    stats = MemoryAccessStatistics()
    with otf2.reader.open(trace) as trace_reader:
        for location, event in trace_reader.events:
            space = AddressSpace(attributes=event.attributes)
            if space.initialized():
                stats.add_mapped_space(space)
            if isinstance(event, otf2.events.Metric) and AccessType.contains(event.metric.member.name):
                stats.add_access(event, location)
    return stats


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

# @app.context_processor
# def utility_processor():
#     def count_access_types_for_space(space_dict):
#         for space in space_dict:
#             yield space.count_access_types()
#     return dict(count_access_types_for_space=count_access_types_for_space)

@app.route('/')
def index():
    index_name = 'Access Statistics'
    chart_name = 'Accesses per space'
    stats = process_trace(app.config['trace'])
    app.config['space_stats'] = stats.get_space_stats()

    space_utilization = defaultdict(int)
    for key, space_list in app.config['space_stats'].items():
        for space in space_list:
            space_utilization[key] = sum([len(access_seq) for _, access_seq in space.get_all_accesses()])

    space_colors = {}
    for i, space_name in enumerate(sorted(space_utilization.keys()), 0):
        space_colors[space_name] = COLORS[i]

    return render_template("index.html", space_stats_dict=space_utilization, space_colors_dict=space_colors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", help="Path to trace file i.e. trace.otf2", type=str)
    args = parser.parse_args()
    if args.trace:
        app.config['trace'] = args.trace
        app.run()
