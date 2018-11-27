
from collections import defaultdict, namedtuple

import otf2
from otf2.enums import Type, LocationType, RecorderKind

class MetricDict:

    AsyncMetric = namedtuple('AsyncMetric', ['location','instance'])

    def __init__(self, trace_writer):
        self._trace_writer = trace_writer
        self._metric_classes = defaultdict()
        self._metric_locations = defaultdict()


    def _get_metric_class(self, metric_name, unit, value_type):
        if  metric_name in self._metric_classes:
            return self._metric_classes[metric_name]
        metric_member = self._trace_writer.definitions.metric_member(metric_name, unit=unit, value_type=value_type)
        self._metric_classes[metric_name] = self._trace_writer.definitions.metric_class((metric_member,),
                                                                                        recorder_kind=RecorderKind.ABSTRACT)
        return self._metric_classes[metric_name]


    def _get_metric_location(self, location_scope, metric_key, metric_class):
        if metric_key in self._metric_locations:
            return self._metric_locations[metric_key]

        location = self._trace_writer.definitions.location(name=metric_key,
                                                            group=location_scope.group,
                                                            type=LocationType.METRIC)

        instance = self._trace_writer.definitions.metric_instance(metric_class=metric_class,
                                                                    recorder=location,
                                                                    scope=location_scope)

        self._metric_locations[metric_key] = self.AsyncMetric(location, instance)

        return self._metric_locations[metric_key]


    def get(self, location_scope, metric_name, metric_key, unit="#", value_type=Type.UINT64):
        metric_class = self._get_metric_class(metric_name, unit, value_type)
        return self._get_metric_location(location_scope, metric_key, metric_class)
