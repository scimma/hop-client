__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # declare namespace

try:
    from ._version import version as __version__
except ImportError:
    pass

from .io import Stream

stream = Stream()
