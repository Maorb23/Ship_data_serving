from __future__ import annotations

import argparse
import fnmatch
import os
from datetime import datetime
from pathlib import Path
import zipfile

EXCLUDED_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".ipynb_checkpoints",
    "build",
    "dist",
    "htmlcov",
    ".eggs",
    "venv",
    ".venv",
    "venvo",
    "venvn",
    "venvb",
}

EXCLUDED_FILE_NAMES = {
    ".DS_Store",
    "Thumbs.db",
    ".coverage",
}

EXCLUDED_FILE_PATTERNS = (
    "*.pyc",
    "*.pyo",
    "*.pyd",
    "*.log",
    "*.tmp",
    "*.temp",
    "*.swp",
    "*.swo",
    "*.egg-info",
)


def is_virtualenv_dir(name: str) -> bool:
    lowered = name.lower()
    return lowered in {"venv", ".venv", "venvo", "venvn", "venvb"} or lowered.startswith(".venv")


def should_exclude(relative_path: Path, is_dir: bool, extra_patterns: tuple[str, ...]) -> bool:
    for part in relative_path.parts:
        if part in EXCLUDED_DIR_NAMES or is_virtualenv_dir(part):
            return True

    if is_dir:
        return False

    if relative_path.name in EXCLUDED_FILE_NAMES:
        return True

    patterns = EXCLUDED_FILE_PATTERNS + extra_patterns
    for pattern in patterns:
        if fnmatch.fnmatch(relative_path.name, pattern):
            return True

    return False


def collect_files(source_dir: Path, output_zip: Path, extra_patterns: tuple[str, ...], verbose: bool) -> list[Path]:
    files_to_zip: list[Path] = []

    for root, dir_names, file_names in os.walk(source_dir):
        root_path = Path(root)
        relative_root = root_path.relative_to(source_dir)

        kept_dirs: list[str] = []
        for dir_name in dir_names:
            relative_dir = relative_root / dir_name if relative_root != Path(".") else Path(dir_name)
            if should_exclude(relative_dir, is_dir=True, extra_patterns=extra_patterns):
                if verbose:
                    print(f"Skipping directory: {relative_dir.as_posix()}")
            else:
                kept_dirs.append(dir_name)

        dir_names[:] = kept_dirs

        for file_name in file_names:
            file_path = root_path / file_name
            if file_path.resolve() == output_zip.resolve():
                continue

            relative_file = file_path.relative_to(source_dir)
            if should_exclude(relative_file, is_dir=False, extra_patterns=extra_patterns):
                if verbose:
                    print(f"Skipping file: {relative_file.as_posix()}")
                continue

            files_to_zip.append(file_path)

    files_to_zip.sort()
    return files_to_zip


def create_zip(source_dir: Path, output_zip: Path, dry_run: bool, extra_patterns: tuple[str, ...], verbose: bool) -> None:
    files_to_zip = collect_files(source_dir, output_zip, extra_patterns, verbose)

    if dry_run:
        print("Dry run. These files would be included:")
        for file_path in files_to_zip:
            print(file_path.relative_to(source_dir).as_posix())
        print(f"Total files: {len(files_to_zip)}")
        return

    output_zip.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_zip, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for file_path in files_to_zip:
            archive.write(file_path, arcname=file_path.relative_to(source_dir).as_posix())

    print(f"Created: {output_zip}")
    print(f"Files added: {len(files_to_zip)}")


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    default_output = script_dir.parent / f"{script_dir.name}_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"

    parser = argparse.ArgumentParser(
        description="Create a clean zip archive of the project without virtual environments and cache files.",
    )
    parser.add_argument(
        "--source",
        default=str(script_dir),
        help="Folder to archive (default: this script's folder).",
    )
    parser.add_argument(
        "--output",
        default=str(default_output),
        help="Output zip path (default: ../<folder>_package_<timestamp>.zip).",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Additional filename glob pattern to exclude (can be used multiple times).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print included files without creating a zip.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print skipped files and directories.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_dir = Path(args.source).expanduser().resolve()
    output_zip = Path(args.output).expanduser().resolve()
    extra_patterns = tuple(args.exclude)

    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError(f"Source folder not found or not a directory: {source_dir}")

    create_zip(
        source_dir=source_dir,
        output_zip=output_zip,
        dry_run=args.dry_run,
        extra_patterns=extra_patterns,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
