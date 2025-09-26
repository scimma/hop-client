# The pypi packages 'bson' and 'pymongo' both provide a 'bson', but with
# different interfaces. See which one we have.

import bson

if hasattr(bson, "dumps"):
    dumps = bson.dumps
    loads = bson.loads
else:
    dumps = bson.encode
    loads = bson.decode
