import argparse
import sys
from subsets_utils import validate_environment

from ingest import oai_harvest


def main():
    parser = argparse.ArgumentParser(description="arXiv Comprehensive Connector")
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--transform-only", action="store_true")
    args = parser.parse_args()

    validate_environment()

    needs_continuation = False

    if not args.transform_only:
        print("\n=== Ingest ===")
        needs_continuation = oai_harvest.run()

    if not args.ingest_only:
        print("\n=== Transform ===")
        print("  (transforms to be added after profiling)")

    if needs_continuation:
        print("\nExiting with code 2 to signal continuation needed")
        sys.exit(2)


if __name__ == "__main__":
    main()
