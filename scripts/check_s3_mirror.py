"""Check that the datasets published in the ClimateBenchPress object store are
identical to a local reference copy of the processed datasets.

Every dataset that is published under `s3://esiwacebucket/ClimateBenchPress/` is
downloaded with `climatebenchpress.data_loader.s3.fetch_standardized` -- i.e. the
very code path that the data loader itself uses -- and then compared against
`<reference>/<name>/standardized.zarr` both byte-for-byte and semantically.

The script exits non-zero if any dataset differs.
"""

import argparse
import hashlib
import sys
import tempfile
from pathlib import Path

import xarray as xr
from climatebenchpress.data_loader import s3

DEFAULT_REFERENCE = Path(
    "/gws/ssde/j25a/aopp/treichelt/ClimateBenchPress/data-loader/datasets"
)

# Directories in the reference tree that are stale leftovers and must not be
# compared against the object store.
IGNORED_REFERENCES = frozenset({"ifs-humidity-old", "ifs-uncompressed-old"})

_HASH_BUFFER_SIZE = 4 * 1024 * 1024


def _digest(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(_HASH_BUFFER_SIZE):
            h.update(chunk)
    return h.hexdigest()


def _relative_files(root: Path) -> dict[str, Path]:
    return {
        str(path.relative_to(root)): path for path in root.rglob("*") if path.is_file()
    }


def compare_bytes(fetched: Path, reference: Path) -> list[str]:
    """Compare the two Zarr stores object by object."""
    ours, theirs = _relative_files(fetched), _relative_files(reference)

    problems = []

    for key in sorted(set(ours) - set(theirs)):
        problems.append(f"only in the object store: {key}")
    for key in sorted(set(theirs) - set(ours)):
        problems.append(f"only in the reference: {key}")

    for key in sorted(set(ours) & set(theirs)):
        mine, yours = ours[key], theirs[key]

        if mine.stat().st_size != yours.stat().st_size:
            problems.append(
                f"size differs: {key} "
                f"({mine.stat().st_size} vs {yours.stat().st_size} bytes)"
            )
        elif _digest(mine) != _digest(yours):
            problems.append(f"content differs: {key}")

    return problems


def _differs(mine: object, yours: object) -> bool:
    # NaN fill values are not equal to themselves, so compare them by repr.
    return mine != yours and repr(mine) != repr(yours)


def compare_semantics(fetched: Path, reference: Path) -> list[str]:
    """Compare the two Zarr stores as xarray datasets, including their encoding."""
    problems = []

    ours = xr.open_zarr(fetched)
    theirs = xr.open_zarr(reference)

    try:
        xr.testing.assert_identical(ours, theirs)
    except AssertionError as err:
        problems.append(f"datasets are not identical: {err}")

    for name in sorted(set(ours.variables) & set(theirs.variables)):
        mine, yours = ours[name], theirs[name]

        if mine.dtype != yours.dtype:
            problems.append(f"{name}: dtype {mine.dtype} vs {yours.dtype}")

        for key in ("chunks", "compressor", "filters", "dtype", "_FillValue"):
            if _differs(mine.encoding.get(key), yours.encoding.get(key)):
                problems.append(
                    f"{name}: encoding[{key!r}] "
                    f"{mine.encoding.get(key)!r} vs {yours.encoding.get(key)!r}"
                )

    return problems


def check(name: str, cache: Path, reference: Path, progress: bool) -> str:
    """Check a single dataset. Returns "ok", "unverified" or "MISMATCH"."""
    print(f"\n=== {name}")

    expected = reference / name / s3.STANDARDIZED
    if not expected.exists():
        print(f"  UNVERIFIED: no reference copy at {expected}")
        return "unverified"

    fetched = cache / name / s3.STANDARDIZED
    if not fetched.exists():
        if not s3.fetch_standardized(name, fetched, progress=progress):
            print(f"  UNVERIFIED: {name} is not published in the object store")
            return "unverified"

    problems = compare_bytes(fetched, expected)
    if problems:
        print(f"  byte comparison: {len(problems)} problem(s)")
        for problem in problems[:20]:
            print(f"    - {problem}")
        if len(problems) > 20:
            print(f"    ... and {len(problems) - 20} more")
    else:
        print("  byte comparison: identical")

    semantic = compare_semantics(fetched, expected)
    if semantic:
        print(f"  semantic comparison: {len(semantic)} problem(s)")
        for problem in semantic[:20]:
            print(f"    - {problem}")
        return "MISMATCH"

    print("  semantic comparison: identical")

    # A pure metadata difference is worth reporting but is not a mismatch.
    return "ok (byte differences)" if problems else "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument(
        "--cache",
        type=Path,
        default=None,
        help="where to download the object store copies to (default: a temp dir)",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="only check these dataset names (default: everything in the bucket)",
    )
    parser.add_argument("--no-progress", action="store_true")
    args = parser.parse_args()

    published = s3.list_published()
    names = args.datasets if args.datasets is not None else published

    print(f"published in the object store: {', '.join(published)}")

    local = {
        path.name
        for path in args.reference.iterdir()
        if path.is_dir() and path.name not in IGNORED_REFERENCES
    }
    print(f"reference copies:              {', '.join(sorted(local))}")

    missing_from_bucket = sorted(local - set(published))
    if missing_from_bucket:
        print(
            f"\nNOT published (will be reprocessed): {', '.join(missing_from_bucket)}"
        )

    with tempfile.TemporaryDirectory() as tmp:
        cache = args.cache if args.cache is not None else Path(tmp)
        cache.mkdir(parents=True, exist_ok=True)

        results = {
            name: check(name, cache, args.reference, not args.no_progress)
            for name in names
        }

    print("\n=== summary")
    for name, result in results.items():
        print(f"  {result:24} {name}")

    failed = [name for name, result in results.items() if result == "MISMATCH"]
    if failed:
        print(f"\nFAILED: {len(failed)} dataset(s) differ: {', '.join(failed)}")
        return 1

    print("\nAll compared datasets match the reference copies.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
