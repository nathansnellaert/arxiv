"""Comprehensive arXiv metadata harvest via OAI-PMH.

Harvests ALL arXiv metadata using OAI-PMH protocol. This fetches the complete
corpus of ~2.5M papers with full metadata (title, authors, abstract, categories, dates).

OAI-PMH docs: https://info.arxiv.org/help/oa/index.html
Rate limit: 3 seconds between requests (arXiv requirement)

This will take 6-12+ hours for a full harvest due to rate limits.
Progress is saved after each batch so it can resume if interrupted.
"""
import xml.etree.ElementTree as ET
import time
import gzip
import json
from pathlib import Path
from subsets_utils import get, get_data_dir, load_state, save_state

BASE_URL = "https://export.arxiv.org/oai2"

# OAI-PMH namespaces
NS = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'arx': 'http://arxiv.org/OAI/arXiv/',
}


def parse_arxiv_record(record_elem) -> dict | None:
    """Parse a single arXiv OAI record into a dict."""
    header = record_elem.find('oai:header', NS)
    if header is None:
        return None

    # Skip deleted records
    if header.get('status') == 'deleted':
        return None

    identifier = header.findtext('oai:identifier', '', NS)
    datestamp = header.findtext('oai:datestamp', '', NS)

    # Extract arXiv ID from OAI identifier (oai:arXiv.org:1234.56789)
    arxiv_id = identifier.replace('oai:arXiv.org:', '') if identifier else None

    record = {
        'id': arxiv_id,
        'datestamp': datestamp,
        'title': None,
        'authors': [],
        'abstract': None,
        'categories': [],
        'primary_category': None,
        'comments': None,
        'journal_ref': None,
        'doi': None,
        'created': None,
        'updated': None,
        'license': None,
    }

    # Get arXiv-specific metadata
    metadata = record_elem.find('oai:metadata', NS)
    if metadata is None:
        return record

    arx = metadata.find('arx:arXiv', NS)
    if arx is None:
        return record

    record['title'] = arx.findtext('arx:title', '', NS).strip().replace('\n', ' ')
    record['abstract'] = arx.findtext('arx:abstract', '', NS).strip()
    record['comments'] = arx.findtext('arx:comments', None, NS)
    record['journal_ref'] = arx.findtext('arx:journal-ref', None, NS)
    record['doi'] = arx.findtext('arx:doi', None, NS)
    record['license'] = arx.findtext('arx:license', None, NS)
    record['created'] = arx.findtext('arx:created', None, NS)
    record['updated'] = arx.findtext('arx:updated', None, NS)

    # Categories
    categories_elem = arx.find('arx:categories', NS)
    if categories_elem is not None and categories_elem.text:
        record['categories'] = categories_elem.text.split()
        if record['categories']:
            record['primary_category'] = record['categories'][0]

    # Authors
    authors_elem = arx.find('arx:authors', NS)
    if authors_elem is not None:
        for author in authors_elem.findall('arx:author', NS):
            keyname = author.findtext('arx:keyname', '', NS)
            forenames = author.findtext('arx:forenames', '', NS)
            name = f"{forenames} {keyname}".strip()
            if name:
                record['authors'].append(name)

    return record


def fetch_batch(resumption_token: str | None = None) -> tuple[list[dict], str | None]:
    """Fetch a batch of records from OAI-PMH endpoint."""
    if resumption_token:
        url = f"{BASE_URL}?verb=ListRecords&resumptionToken={resumption_token}"
    else:
        # Initial request - use arXiv native format for richest metadata
        url = f"{BASE_URL}?verb=ListRecords&metadataPrefix=arXiv"

    response = get(url, timeout=120)

    if response.status_code == 503:
        # Server busy, wait and retry
        retry_after = int(response.headers.get('Retry-After', 30))
        print(f"    Server busy, waiting {retry_after}s...")
        time.sleep(retry_after)
        return fetch_batch(resumption_token)

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}: {response.text[:500]}")

    root = ET.fromstring(response.content)

    # Check for errors
    error = root.find('.//oai:error', NS)
    if error is not None:
        raise Exception(f"OAI error: {error.text} (code: {error.get('code')})")

    # Parse records
    records = []
    for record_elem in root.findall('.//oai:record', NS):
        record = parse_arxiv_record(record_elem)
        if record:
            records.append(record)

    # Get resumption token
    token_elem = root.find('.//oai:resumptionToken', NS)
    next_token = None
    if token_elem is not None and token_elem.text:
        next_token = token_elem.text

    return records, next_token


def run():
    """Harvest all arXiv metadata via OAI-PMH."""
    print("Harvesting arXiv metadata via OAI-PMH...")
    print("  This will take 6-12+ hours for full corpus (~2.5M papers)")

    data_dir = Path(get_data_dir()) / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Load state for resumption
    state = load_state("oai_harvest")
    resumption_token = state.get("resumption_token")
    total_harvested = state.get("total_harvested", 0)
    batch_num = state.get("batch_num", 0)

    if resumption_token:
        print(f"  Resuming from batch {batch_num}, {total_harvested:,} records so far...")

    # Output file - append mode with gzip
    output_file = data_dir / "arxiv_metadata.jsonl.gz"
    mode = 'ab' if resumption_token else 'wb'

    with gzip.open(output_file, mode) as f:
        while True:
            batch_num += 1

            try:
                records, next_token = fetch_batch(resumption_token)
            except Exception as e:
                print(f"  Error fetching batch {batch_num}: {e}")
                print("  Saving progress and stopping...")
                save_state("oai_harvest", {
                    "resumption_token": resumption_token,
                    "total_harvested": total_harvested,
                    "batch_num": batch_num - 1,
                })
                raise

            # Write records
            for record in records:
                f.write(json.dumps(record).encode('utf-8') + b'\n')

            total_harvested += len(records)
            print(f"  Batch {batch_num}: +{len(records)} records (total: {total_harvested:,})")

            # Save progress after each batch
            save_state("oai_harvest", {
                "resumption_token": next_token,
                "total_harvested": total_harvested,
                "batch_num": batch_num,
            })

            if not next_token:
                print(f"  Harvest complete! Total: {total_harvested:,} records")
                break

            resumption_token = next_token

            # Rate limit - arXiv requires 3s between requests
            time.sleep(3)

    print(f"  Output: {output_file}")
    print(f"  Size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
