__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # declare namespace

from ._version import version as __version__

from .io import Stream

stream = Stream()
