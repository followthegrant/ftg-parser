from pathlib import Path

from followthegrant import settings

from ..exceptions import ParserException
from ..logging import get_logger
from ..transform import EGenerator, make_proxies
from . import parsers

log = get_logger(__name__)


def _exists(fpath: Path) -> bool:
    if not fpath.exists():
        log.error(f"Path `{fpath}` does not exist.", data_root=settings.DATA_ROOT)
        return False
    return True


def parse(fpath: str | Path, parser: str, dataset: str | None = None) -> EGenerator:
    """
    data input: output from any of the parsers in `ftg.parse.parsers`
    """
    parser_ = getattr(parsers, parser, None)
    if parser_ is None:
        raise ParserException(f"Unknown parser: `{parser}`")
    fpath = Path(fpath)
    if not _exists(fpath):
        return

    for result in parser_(fpath):
        yield from make_proxies(result, dataset)
