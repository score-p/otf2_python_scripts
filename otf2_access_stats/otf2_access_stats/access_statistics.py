"""
Flask application for analyzing/visualizing memory accesses
to memory mapped files recorded and stored in OTF2 traces.
"""

from contextlib import closing
from collections import defaultdict
from flask import Flask, jsonify, render_template, request

import otf2
from otf2.enums import LocationType
from otf2.events import Enter, Leave

from .spacecollection import AddressSpace, AccessType, is_flush
from .spacestatistics import MemoryAccessStatistics

COLORS = ["#0000FF",
          "#00FF00",
          "#FFFF00",
          "#FF8000",
          "#FF0000",
          "#7F00FF",
          "#00FF00",
          "#808080"]


def create_app(trace_file: str):
    """
    Creates flask application.
    """
    app = Flask(__name__)

    def process_trace(trace, app):
        func_stack = list()
        trace_reader = otf2.reader.Reader(trace)
        with closing(trace_reader):
            stats = MemoryAccessStatistics(trace_reader.timer_resolution)
            for location, event in trace_reader.events:
                if isinstance(event, Enter) and is_flush(event.region.name):
                    func_stack.append(event)
                if isinstance(event, Leave) and is_flush(event.region.name):
                    stats.add_flush(func_stack.pop(), event)
                if (location.type == LocationType.CPU_THREAD and
                        not location.name in app.config['location_mapping']):
                    app.config['location_mapping'][location.name] = location
                space = AddressSpace(attributes=event.attributes)
                if space.initialized():
                    stats.add_mapped_space(space)
                if (isinstance(event, otf2.events.Metric) and
                        AccessType.contains(event.metric.member.name)):
                    stats.add_access(event, event.metric.scope)
        return stats

    def get_space_colors():
        space_colors = {}
        for i, space_name in enumerate(sorted(app.config['space_stats'].keys()), 0):
            space_colors[space_name] = COLORS[i]
        return space_colors

    @app.route('/_get_space_util_for_thread')
    def get_space_util_for_thread():
        selected_thread = request.args.get('selected_thread', 0, type=str)
        location = app.config['location_mapping'][selected_thread]
        stats_per_source = app.config['memory_access_stats'].resource_utilization(location)
        colors = [app.config['src_colors'][s] for s in stats_per_source]
        return jsonify(labels=list(stats_per_source.keys()),
                       colors=colors,
                       data=list(stats_per_source.values()))

    @app.route('/_get_thread_stats_per_space')
    def get_thread_stats_per_space():
        selected_space = request.args.get('selected_space', 0, type=str)
        (stores, loads) = app.config['memory_access_stats'].thread_access_stats(selected_space)
        return jsonify(labels=list(loads.keys()),
                       loads=list(loads.values()),
                       stores=list(stores.values()))


    @app.route('/_get_resource_summary')
    def get_resource_summary():
        selected_src = request.args.get('selected_res', 0, type=str)
        summary = app.config['memory_access_stats'].resource_summary(selected_src)
        return jsonify(summary=summary)

    @app.context_processor
    def utility_processor():

        def get_space_utilization():
            space_utilization = defaultdict(int)
            for key, space_list in app.config['space_stats'].items():
                for space in space_list:
                    space_utilization[key] = sum([len(access_seq) for _, access_seq
                                                  in space.get_all_accesses()])
            return space_utilization

        def get_space_lengths():
            return [sum([space.size for space in spaces])
                    for spaces in app.config['space_stats'].values()]

        return dict(get_space_utilization=get_space_utilization,
                    get_space_lengths=get_space_lengths,
                    sorted=sorted)

    @app.route('/')
    def index():
        return render_template("index.html")

    app.config['location_mapping'] = dict()
    app.config['location_mapping']['ALL'] = None
    app.config['trace'] = trace_file
    app.config['memory_access_stats'] = process_trace(app.config['trace'], app)
    app.config['space_stats'] = app.config['memory_access_stats'].get_space_stats()
    app.config['src_colors'] = get_space_colors()
    app.config['default_resource'] = list(app.config['space_stats'].keys())[0]

    return app
