"""Pipeline execution stages (01..06)."""

from importlib import import_module

_stage01 = import_module(".01_fetch", package=__name__)
_stage02 = import_module(".02_scan", package=__name__)
_stage03 = import_module(".03_validate", package=__name__)
_stage04 = import_module(".04_revalidate", package=__name__)
_stage05 = import_module(".05_delete", package=__name__)
_stage06 = import_module(".06_restore", package=__name__)

run_fetch = _stage01.run_fetch
run_scan = _stage02.run_scan
run_validate = _stage03.run_validate
run_revalidate = _stage04.run_revalidate
run_delete = _stage05.run_delete
run_restore = _stage06.run_restore

__all__ = [
    "run_fetch",
    "run_scan",
    "run_validate",
    "run_revalidate",
    "run_delete",
    "run_restore",
]
