from collections import defaultdict, namedtuple, OrderedDict
from enum import Enum, auto

MMAP_SIZE_TAG = "mappedSize"
MMAP_ADDRESS_TAG = "startAddress"
MMAP_SOURCE_TAG = "mappedSource"
SCOREP_MEMORY_ADDRESS = "scorep:memoryaddress:begin"
SCOREP_MEMORY_SIZE = "scorep:memoryaddress:len"

Access = namedtuple('Access', ['address','type'])


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


class AddressSpace:
    """
    Stores details of used address space.
    """

    def _init_by_attributes(self, attributes):
        for attribute in attributes:
            if attribute.name == MMAP_SIZE_TAG:
                self.Size = attributes[attribute]
            elif attribute.name == MMAP_ADDRESS_TAG:
                self.Address = attributes[attribute]
            elif attribute.name == MMAP_SOURCE_TAG:
                self.Source = attributes[attribute]


    def _init_by_properties(self, properties):
        for prop in trace.definitions.location_properties:
            if prop.name == SCOREP_MEMORY_ADDRESS:
                self.Address = int(prop.value)
            elif prop.name == SCOREP_MEMORY_SIZE:
                self.Size = int(prop.value)
        self.Source = "Score-P"


    def __init__(self, attributes=None, properties=None):
        self.Size = -1
        self.Source = ""
        self.Address = -1
        self.Accesses = defaultdict(AccessSequence)
        if attributes:
            self._init_by_attributes(attributes)
        elif properties:
            self._init_by_properties(properties)


    def add_access_on_location(self, timestamp, access, location):
        self.Accesses[location].add(timestamp, access)


    def get_all_accesses(self):
        for location, access_seq in self.Accesses.items():
            yield location, access_seq


    def initialized(self):
        return self.Size != -1 and self.Address != -1


    def __str__(self):
        return "[{}, {}] = Size: {}, Source {}, {}".format(
            self.Address,
            self.Address + self.Size,
            self.Size,
            self.Source)