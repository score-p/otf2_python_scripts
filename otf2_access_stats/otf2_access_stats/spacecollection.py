from collections import defaultdict, namedtuple, OrderedDict
from enum import Enum, auto
from intervaltree import Interval

from . import config as conf

Access = namedtuple('Access', ['address', 'type'])


class TimeStamp:
    def __init__(self, resolution, ticks):
        self._ticks = ticks
        self._resolution = resolution


    def __str__(self):
        time = self.nsec()
        for unit in ["ns", "us", "ms", "s"]:
            if time < 1000:
                return "{} {}".format(time, unit)
            time /= 1000
        return "{} {}".format(time, unit)


    def ticks(self):
        return self._ticks


    def nsec(self):
        return int(round(self._ticks / (self._resolution / 1e9), 0))


    def usec(self):
        return int(round(self._ticks / (self._resolution / 1e6), 0))


    def sec(self):
        return int(round(self._ticks / self._resolution, 0))


    def __add__(self, other):
        return TimeStamp(self._resolution, self._ticks + other.ticks())


    def __sub__(self, other):
        return TimeStamp(self._resolution, self._ticks - other.ticks())


    def __lt__(self, other):
        return self._ticks < other.ticks()


class Flush:
    def __init__(self, attributes, t_begin, t_end):
        def find_attribute(name):
            return next(attributes[k] for k in attributes if k.name == name)

        assert t_begin < t_end

        self._t_begin = t_begin
        self._t_end = t_end

        try:
            addr_begin = int(find_attribute(conf.FLUSH_ADDRESS_ATTR))
            addr_end = addr_begin + int(find_attribute(conf.FLUSH_LENGTH_ATTR))
            self._interval = Interval(addr_begin, addr_end)
        except StopIteration:
            print("Could not find flush attributes on event")
            raise ValueError


    def duration(self):
        return self._t_end - self._t_begin


    def interval(self):
        return self._interval


def is_flush(func_name: str):
    return func_name in conf.FLUSH_OPERATIONS


def get_flush_range(flush_space: Interval, address_space: Interval) -> int:
    if flush_space.distance_to(address_space) > 0 or flush_space.distance_to(address_space) > 0:
        raise ValueError
    elif flush_space.contains_interval(address_space):
        return address_space
    elif address_space.contains_interval(flush_space):
        return flush_space
    elif flush_space < address_space:
        return Interval(address_space.begin, flush_space.end)
    elif flush_space > address_space:
        return Interval(flush_space.begin, address_space.end)


class AccessType(Enum):
    """
    Enum class to distinguish between access types.
    """

    LOAD = auto()
    STORE = auto()
    INVALID = auto()

    @classmethod
    def get_by_name(cls, type_name):
        if conf.LOAD_METRIC_LABEL in type_name:
            return cls.LOAD
        elif conf.STORE_METRIC_LABEL in type_name:
            return cls.STORE
        return cls.INVALID

    @classmethod
    def contains(cls, type_name):
        return cls.get_by_name(type_name) != cls.INVALID


class AccessSequence:
    """
    Stores a sequence of Access's by timestamp.
    """
    def __init__(self):
        self._accesses = OrderedDict()


    def add(self, timestamp, access):
        self._accesses[timestamp] = access


    def get(self):
        for time, access in self._accesses.items():
            yield time, access


    def __len__(self):
        return len(self._accesses)


    def __str__(self):
        out = "("
        for timestamp, access in self._accesses.items():
            out += "({} : {}, {}),".format(timestamp, access.type, access.address)
        return out + ")"


def count_stores(access_seq):
    return sum(1 for _, access in access_seq.get() if access.type == AccessType.STORE)


def count_loads(access_seq):
    return sum(1 for _, access in access_seq.get() if access.type == AccessType.LOAD)


class AddressSpace:
    """
    Stores details of used address space.
    """

    def _init_by_attributes(self, attributes):
        for attribute in attributes:
            if attribute.name == conf.MMAP_SIZE_TAG:
                self.size = attributes[attribute]
            elif attribute.name == conf.MMAP_ADDRESS_TAG:
                self.start_address = attributes[attribute]
            elif attribute.name == conf.MMAP_SOURCE_TAG:
                self.source = attributes[attribute]


    def _init_by_properties(self, properties):
        for prop in properties:
            if prop.name == conf.SCOREP_MEMORY_ADDRESS:
                self.start_address = int(prop.value)
            elif prop.name == conf.SCOREP_MEMORY_SIZE:
                self.size = int(prop.value)
        self.source = "Score-P"


    def __init__(self, attributes=None, properties=None):
        self.flushed_data = 0
        self.size = -1
        self.source = ""
        self.start_address = -1
        self.accesses = defaultdict(AccessSequence)
        if attributes:
            self._init_by_attributes(attributes)
        elif properties:
            self._init_by_properties(properties)
        self._interval = Interval(self.start_address, self.start_address + self.size)
        self._flushed_ranges = list()


    def add_access_on_location(self, timestamp, access, location):
        self.accesses[location].add(timestamp, access)


    def get_all_accesses(self):
        for location, access_seq in self.accesses.items():
            yield location, access_seq


    def flush(self, flush_event):
        flushed_range = get_flush_range(flush_event.interval(), self._interval)
        self.flushed_data += flushed_range.length()
        self._flushed_ranges.append(flushed_range)


    def flushs(self):
        for interval in self._flushed_ranges:
            yield interval


    def initialized(self):
        return self.size != -1 and self.start_address != -1


    def __getitem__(self, location):
        return self.accesses[location]


    def __str__(self):
        return "[{}, {}] = size: {}, source {}".format(self.start_address,
                                                       self.start_address + self.size,
                                                       self.size,
                                                       self.source)
