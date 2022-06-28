import builtins
from collections import OrderedDict


class HashableList(list):
    def __hash__(self):
        h = 0
        for v in self:
            h = hash(h ^ hash(v))
        return h


class HashableDict(OrderedDict):
    def __hash__(self):
        h = 0
        for k, v in self.items():
            h = hash(h ^ hash(k) ^ hash(v))
        return h


class SchemaGenerator(object):
    def __init__(self):
        # a map of visited object ids to types
        self.type_cache = {}
        self.auto_record_counter = 0

    def determine_type(self, value):
        value_id = id(value)
        if value_id in self.type_cache:
            return self.type_cache[value_id]
        type = None
        if isinstance(value, str):
            type = "string"
        elif isinstance(value, bytes):
            type = "bytes"
        elif value is None:
            type = "null"
        elif value is True:
            type = "boolean"
        elif value is False:
            type = "boolean"
        elif isinstance(value, int):
            # TODO: deal with range, int/long?
            type = "long"
        elif isinstance(value, float):
            # TODO: deal with range, float/double?
            type = "double"
        elif isinstance(value, dict):
            if not all(isinstance(k, str) for k in value.keys()):
                bad_key = next(filter(lambda k: not isinstance(k, str), value.keys()))
                raise ValueError("Dictionaries with non-string keys cannot be represented as Avro. "
                                 f"Offending key: {bad_key}")

            item_type = self.find_common_type(value.values())

            # If the resulting item type is a union, it may or may not be a good idea.
            # If the input were {"a": 1, "b": "2", "c": 3, "d": "4", . . . } with a lot of keys with
            # values of a small set of types, it may make sense, but for
            # {"name": "Bob", "age": 40, "height": 2.05}, it's mostly pointless and awkward.
            # So, we try to apply a heuristic to get this right some of the time.
            # Having a large number of value types relative to the number of entries is indicative
            # of more properly being a record (struct) type
            if isinstance(item_type, list) and len(item_type) > len(value) // 2:
                record_name = f"auto_record{self.auto_record_counter}"
                self.auto_record_counter += 1

                type = HashableDict(
                    type="record",
                    name=record_name,
                    fields=HashableList(HashableDict(name=k, type=self.determine_type(v))
                                        for k, v in value.items())
                )
            else:
                type = HashableDict(type="map", values=item_type)
        elif isinstance(value, (list, tuple)):
            item_type = self.find_common_type(value)
            type = HashableDict(type="array", items=item_type)
        else:
            raise ValueError("Unable to assign an Avro type to value of type "
                             f"{builtins.type(value)}")
        self.type_cache[value_id] = type

        return type

    def find_common_type(self, values):
        types = set()
        for value in values:
            types.add(self.determine_type(value))
        if len(types) == 1:
            return types.pop()  # only one item in set, so pop must yield it
        else:
            return HashableList(types)  # form a union type
