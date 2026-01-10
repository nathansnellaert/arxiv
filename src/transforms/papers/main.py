"""Transform arXiv metadata into papers dataset (incremental with DuckDB).

- Diffs ingest state vs transform state to find new dates
- Uses DuckDB for efficient transformation
- Merges to Delta table by ID
"""
import duckdb
from subsets_utils import load_state, save_state, upload_data, sync_metadata
from subsets_utils.duckdb import raw
from .test import test

METADATA = {
    "title": "arXiv Papers",
    "description": "arXiv preprint metadata. Updated incrementally via OAI-PMH.",
}


def run():
    """Transform new dates incrementally."""
    print("  Transforming arXiv papers...")

    # Diff ingest vs transform state
    ingest_state = load_state("oai_harvest")
    transform_state = load_state("transform_papers")

    fetched = set(ingest_state.get("fetched_dates", []))
    transformed = set(transform_state.get("transformed_dates", []))
    new_dates = sorted(fetched - transformed)

    if not new_dates:
        print("  No new dates to transform")
        return

    print(f"  Processing {len(new_dates)} new dates in single query")

    # Transform all new dates in one DuckDB query
    assets = [f"papers/{d}" for d in new_dates]
    table = duckdb.sql(f"""
        SELECT
            id, datestamp, title, abstract,
            array_to_string(authors, ', ') as authors,
            array_to_string(categories, ' ') as categories,
            primary_category, comments, journal_ref, doi, created, updated, license
        FROM {raw(assets)}
    """).arrow()

    print(f"  {table.num_rows} total records")

    test(table)
    upload_data(table, "arxiv_papers", mode="merge", merge_key="id")

    # Update state with all new dates
    transformed.update(new_dates)
    save_state("transform_papers", {"transformed_dates": sorted(transformed)})

    sync_metadata("arxiv_papers", METADATA)
    print("  Done!")
