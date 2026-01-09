import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date


def test(table: pa.Table) -> None:
    """Validate arxiv papers dataset."""
    validate(table, {
        "columns": {
            "id": "string",
            "title": "string",
            "abstract": "string",
            "authors": "string",
            "primary_category": "string",
            "categories": "string",
            "created": "string",
            "updated": "string",
            "doi": "string",
            "journal_ref": "string",
            "comments": "string",
            "license": "string",
        },
        "not_null": ["id", "title", "created", "primary_category"],
        "unique": ["id"],
        "min_rows": 100000,
    })

    assert_valid_date(table, "created")
