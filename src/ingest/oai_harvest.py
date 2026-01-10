"""arXiv metadata harvest via OAI-PMH with date-based partitioning.

- Uses T-2 (day before yesterday) for timezone safety
- Saves parquet per day: papers_YYYY-MM-DD.parquet
- Tracks fetched_dates in state for transform to diff against
"""
import xml.etree.ElementTree as ET
import time
from datetime import date, timedelta
import httpx
import pyarrow as pa
from subsets_utils import get, load_state, save_state, save_raw_parquet

BASE_URL = "https://export.arxiv.org/oai2"
GH_ACTIONS_MAX_RUN_SECONDS = 5.5 * 60 * 60

NS = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'arx': 'http://arxiv.org/OAI/arXiv/',
}

SCHEMA = pa.schema([
    ('id', pa.string()),
    ('datestamp', pa.string()),
    ('title', pa.string()),
    ('authors', pa.list_(pa.string())),
    ('abstract', pa.string()),
    ('categories', pa.list_(pa.string())),
    ('primary_category', pa.string()),
    ('comments', pa.string()),
    ('journal_ref', pa.string()),
    ('doi', pa.string()),
    ('created', pa.string()),
    ('updated', pa.string()),
    ('license', pa.string()),
])


def parse_record(record_elem) -> dict | None:
    """Parse a single arXiv OAI record."""
    header = record_elem.find('oai:header', NS)
    if header is None or header.get('status') == 'deleted':
        return None

    identifier = header.findtext('oai:identifier', '', NS)
    arxiv_id = identifier.replace('oai:arXiv.org:', '') if identifier else None

    record = {
        'id': arxiv_id,
        'datestamp': header.findtext('oai:datestamp', '', NS),
        'title': None, 'authors': [], 'abstract': None, 'categories': [],
        'primary_category': None, 'comments': None, 'journal_ref': None,
        'doi': None, 'created': None, 'updated': None, 'license': None,
    }

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

    categories_elem = arx.find('arx:categories', NS)
    if categories_elem is not None and categories_elem.text:
        record['categories'] = categories_elem.text.split()
        if record['categories']:
            record['primary_category'] = record['categories'][0]

    authors_elem = arx.find('arx:authors', NS)
    if authors_elem is not None:
        for author in authors_elem.findall('arx:author', NS):
            keyname = author.findtext('arx:keyname', '', NS)
            forenames = author.findtext('arx:forenames', '', NS)
            name = f"{forenames} {keyname}".strip()
            if name:
                record['authors'].append(name)

    return record


def fetch_page(target_date: date = None, resumption_token: str = None) -> tuple[list[dict], str | None]:
    """Fetch a page of records from OAI-PMH."""
    if resumption_token:
        url = f"{BASE_URL}?verb=ListRecords&resumptionToken={resumption_token}"
    else:
        date_str = target_date.isoformat()
        url = f"{BASE_URL}?verb=ListRecords&metadataPrefix=arXiv&from={date_str}&until={date_str}"

    for attempt in range(3):
        try:
            response = get(url, timeout=120)
            break
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            if attempt == 2:
                raise
            time.sleep(30 * (attempt + 1))

    if response.status_code == 503:
        time.sleep(int(response.headers.get('Retry-After', 30)))
        return fetch_page(target_date, resumption_token)

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}: {response.text[:500]}")

    root = ET.fromstring(response.content)

    error = root.find('.//oai:error', NS)
    if error is not None:
        if error.get('code') == 'noRecordsMatch':
            return [], None
        raise Exception(f"OAI error: {error.text}")

    records = [r for elem in root.findall('.//oai:record', NS) if (r := parse_record(elem))]

    token_elem = root.find('.//oai:resumptionToken', NS)
    next_token = token_elem.text if token_elem is not None and token_elem.text else None

    return records, next_token


def fetch_date(target_date: date) -> list[dict]:
    """Fetch all records for a date, handling pagination."""
    all_records = []
    token = None
    page = 0

    while True:
        page += 1
        records, token = fetch_page(target_date if page == 1 else None, token)
        all_records.extend(records)
        print(f"+{len(records)}", end=" " if token else "\n")
        if not token:
            break
        time.sleep(3)

    return all_records


def run() -> bool:
    """Harvest arXiv metadata by date. Returns True if more work to do."""
    print("Harvesting arXiv metadata (date-partitioned)...")
    start_time = time.time()

    state = load_state("oai_harvest")
    last_fetched = state.get("last_fetched_date")
    fetched_dates = set(state.get("fetched_dates", []))

    target_end = date.today() - timedelta(days=2)  # T-2 for timezone safety
    start_date = date.fromisoformat(last_fetched) + timedelta(days=1) if last_fetched else date(1991, 1, 1)

    if start_date > target_end:
        print(f"  Up to date (last: {last_fetched})")
        return False

    print(f"  Fetching {start_date} to {target_end}")

    current = start_date
    while current <= target_end:
        if time.time() - start_time >= GH_ACTIONS_MAX_RUN_SECONDS:
            print(f"  Time budget exhausted")
            return True

        print(f"  {current}: ", end="")
        records = fetch_date(current)

        if records:
            table = pa.Table.from_pylist(records, schema=SCHEMA)
            save_raw_parquet(table, f"papers/{current.isoformat()}")
            fetched_dates.add(current.isoformat())

        save_state("oai_harvest", {
            "last_fetched_date": current.isoformat(),
            "fetched_dates": sorted(fetched_dates)
        })

        current += timedelta(days=1)
        time.sleep(3)

    print(f"  Complete!")
    return False
