from importlib import metadata

import fasttext

__version__ = metadata.version(__name__)
fasttext.FastText.eprint = lambda x: None
