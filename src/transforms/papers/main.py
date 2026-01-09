"""Transform arXiv metadata into papers dataset.

Creates a clean dataset of arXiv paper metadata with:
- Paper ID, title, abstract
- Authors (as comma-separated string)
- Categories (primary and all)
- Dates (created, updated)
- Optional DOI, journal reference, comments, license
"""
import gzip
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
from subsets_utils import get_data_dir, sync_data, sync_metadata
from subsets_utils.r2 import is_cloud_mode, download_bytes, get_connector_name
from .test import test


METADATA = {
    "id": "arxiv_papers",
    "title": "arXiv Papers",
    "description": "Comprehensive metadata for arXiv preprints including title, abstract, authors, categories, and dates. Updated via OAI-PMH harvest.",
    "column_descriptions": {
        "id": "arXiv paper ID (e.g., 2301.00001)",
        "title": "Paper title",
        "abstract": "Paper abstract",
        "authors": "Comma-separated list of author names",
        "primary_category": "Primary arXiv category (e.g., cs.AI, math.CO)",
        "categories": "All arXiv categories (space-separated)",
        "created": "Date paper was first submitted (YYYY-MM-DD)",
        "updated": "Date of latest version (YYYY-MM-DD or null if never updated)",
        "doi": "DOI if available",
        "journal_ref": "Journal reference if published",
        "comments": "Author comments (page count, figures, etc.)",
        "license": "License URL",
    }
}


def load_raw_data() -> list[dict]:
    """Load raw arxiv metadata from gzipped JSONL file."""
    if is_cloud_mode():
        key = f"{get_connector_name()}/data/raw/arxiv_metadata.jsonl.gz"
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw data not found in R2: {key}")
        lines = gzip.decompress(data).decode('utf-8').strip().split('\n')
    else:
        raw_path = Path(get_data_dir()) / "raw" / "arxiv_metadata.jsonl.gz"
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw data not found: {raw_path}")
        with gzip.open(raw_path, 'rt', encoding='utf-8') as f:
            lines = f.read().strip().split('\n')

    return [json.loads(line) for line in lines if line.strip()]


def run():
    """Transform arXiv metadata into papers dataset."""
    print("  Loading raw arXiv metadata...")
    raw_data = load_raw_data()
    print(f"  Loaded {len(raw_data):,} papers")

    records = []
    for item in raw_data:
        # Join authors list into comma-separated string
        authors = ', '.join(item.get('authors', [])) if item.get('authors') else None

        # Join categories list into space-separated string
        categories = ' '.join(item.get('categories', [])) if item.get('categories') else None

        records.append({
            'id': item.get('id'),
            'title': item.get('title'),
            'abstract': item.get('abstract'),
            'authors': authors,
            'primary_category': item.get('primary_category'),
            'categories': categories,
            'created': item.get('created'),
            'updated': item.get('updated'),
            'doi': item.get('doi'),
            'journal_ref': item.get('journal_ref'),
            'comments': item.get('comments'),
            'license': item.get('license'),
        })

    table = pa.Table.from_pylist(records)

    # Sort by created date descending (newest first)
    sort_indices = pc.sort_indices(table, [("created", "descending"), ("id", "ascending")])
    table = pc.take(table, sort_indices)

    print(f"  Papers dataset: {len(table):,} rows")

    test(table)
    sync_data(table, "arxiv_papers")
    sync_metadata("arxiv_papers", METADATA)


if __name__ == "__main__":
    run()
