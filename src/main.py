import argparse
from subsets_utils import validate_environment

from ingest import papers as ingest_papers


def main():
    parser = argparse.ArgumentParser(description="arXiv Comprehensive Connector")
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--transform-only", action="store_true")
    args = parser.parse_args()

    validate_environment()

    if not args.transform_only:
        print("\n=== Ingest ===")
        ingest_papers.run()

    if not args.ingest_only:
        print("\n=== Transform ===")
        print("  (transforms to be added after profiling)")


if __name__ == "__main__":
    main()
