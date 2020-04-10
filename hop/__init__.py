try:
    from ._version import version as __version__
except ImportError:
    pass

from .io import Stream

stream = Stream()
