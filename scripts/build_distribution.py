"""Build a submission ZIP from code, configuration and generated model artifacts."""


from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        "--output_dir",
        dest="output_dir",
        type=Path,
        default=Path("output/lab5"),
    )
    parser.add_argument("--destination", type=Path, default=Path("dist/lab5-model"))

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    required = [args.output_dir / "model", args.output_dir / "metrics.json"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit(f"Train the model first; missing: {', '.join(missing)}")

    staging = args.destination
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    shutil.copytree("src", staging / "src")
    shutil.copytree("config", staging / "config")
    shutil.copytree(args.output_dir / "model", staging / "model")
    shutil.copy2(args.output_dir / "metrics.json", staging / "metrics.json")
    shutil.copy2("pyproject.toml", staging / "pyproject.toml")

    archive = shutil.make_archive(str(staging), "zip", root_dir=staging)
    print(f"Created {archive}")


if __name__ == "__main__":
    main()
