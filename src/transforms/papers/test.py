import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate arxiv papers batch."""
    validate(table, {
        "columns": {
            "id": "string",
            "title": "string",
            "abstract": "string",
            "authors": "string",
            "primary_category": "string",
            "created": "string",
        },
        "not_null": ["id", "title", "created", "primary_category"],
    })
