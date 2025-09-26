# The pypi packages 'bson' and 'pymongo' both provide a 'bson', but with
# different interfaces. Use whichever is available, or raise an error if neither
# is installed.

try:
    import bson

    if hasattr(bson, "dumps"):
        dumps = bson.dumps
        loads = bson.loads
    else:
        dumps = bson.encode
        loads = bson.decode
except ImportError:

    def _no_bson_module(doc):
        raise NotImplementedError(
            "No bson package installed. Install hop-client with either the bson or cbson extra"
        )

    dumps = _no_bson_module
    loads = _no_bson_module
