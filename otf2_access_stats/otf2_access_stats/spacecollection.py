from collections import defaultdict, namedtuple, OrderedDict
from enum import Enum, auto
from intervaltree import Interval

import config as conf

Access = namedtuple('Access', ['address','type'])


class TimeStamp:
    def __init__(self, resolution, ticks):
        self._ticks = ticks
        self._resolution = resolution


    def ticks(self):
        return self._ticks


    def usec(self):
        return int(round(self._ticks / (self._resolution / 1000000), 0))


    def sec(self):
        return int(round(self._ticks / (self._resolution ), 0))


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

        assert(t_begin < t_end)
        self._t_begin = t_begin
        self._t_end = t_end

        try:
            addr_begin = int(find_attribute(conf.FLUSH_ADDRESS_ATTR))
            addr_end = addr_begin + int(find_attribute(conf.FLUSH_LENGTH_ATTR))
            self._interval = Interval(addr_begin, addr_end)
        except StopIteration:
            print("Could not find flush attributes on event")
            raise ValueError


    def Duration(self):
        return self._t_end - self._t_begin


    def Interval(self):
        return self._interval


def isFlush(func_name: str):
    return func_name in conf.FLUSH_OPERATIONS


def get_flush_range(f: Interval, address_space: Interval) -> int:
    if 0 < f.distance_to(address_space) or f.distance_to(address_space) > 0:
        raise ValueError
    elif f.contains_interval(address_space):
        return address_space
    elif address_space.contains_interval(f):
        return f
    elif f < address_space:
        return Interval(address_space.begin, f.end)
    elif f > address_space:
        return Interval(f.begin, address_space.end)


class AccessType (Enum):
    """
    Enum class to distinguish between access types.
    """

    LOAD = auto()
    STORE = auto()
    INVALID = auto()

    @classmethod
    def get_by_name(cls, type_name):
        if type_name == "MemoryAccess:load":
            return cls.LOAD
        elif type_name == "MemoryAccess:store":
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
        for t, a in self._accesses.items():
            yield t, a


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
                self.Size = attributes[attribute]
            elif attribute.name == conf.MMAP_ADDRESS_TAG:
                self.Address = attributes[attribute]
            elif attribute.name == conf.MMAP_SOURCE_TAG:
                self.Source = attributes[attribute]


    def _init_by_properties(self, properties):
        for prop in trace.definitions.location_properties:
            if prop.name == conf.SCOREP_MEMORY_ADDRESS:
                self.Address = int(prop.value)
            elif prop.name == conf.SCOREP_MEMORY_SIZE:
                self.Size = int(prop.value)
        self.Source = "Score-P"


    def __init__(self, attributes=None, properties=None):
        self.FlushedData = 0
        self.Size = -1
        self.Source = ""
        self.Address = -1
        self.Accesses = defaultdict(AccessSequence)
        if attributes:
            self._init_by_attributes(attributes)
        elif properties:
            self._init_by_properties(properties)
        self._interval = Interval(self.Address, self.Address + self.Size)
        self._flushed_ranges = list()



    def add_access_on_location(self, timestamp, access, location):
        self.Accesses[location].add(timestamp, access)


    def get_all_accesses(self):
        for location, access_seq in self.Accesses.items():
            yield location, access_seq


    def count_access_types(self):
        nloads = 0
        n = 0
        for location, access_seq in self.Accesses.items():
            nloads += count_loads(access_seq)
            n += len(access_seq)
        return nloads, n - nloads


    def flush(self, f):
        r = get_flush_range(f.Interval(), self._interval)
        self.FlushedData += r.length()
        self._flushed_ranges.append(r)


    def flushs(self):
        for interval in self._flushed_ranges:
            yield interval


    def initialized(self):
        return self.Size != -1 and self.Address != -1


    def __getitem__(self, location):
        return self.Accesses[location]


    def __str__(self):
        return "[{}, {}] = Size: {}, Source {}".format(
            self.Address,
            self.Address + self.Size,
            self.Size,
            self.Source)