"""Dataset configuration and canonical naming patterns for public exports."""

from dataclasses import dataclass
from datetime import datetime


EXPORT_DATE_FORMAT = "%Y_%m_%d"


@dataclass
class PublicDataset:
    """Definition of a public dataset for export."""

    slug: str
    """Canonical slug identifier (e.g., 'store-apps', 'descriptions')."""
    description: str
    """Human-readable description of the dataset."""
    public: bool = True
    """Whether this is a public dataset (as opposed to private/paid)."""


# Registry of all public datasets
PUBLIC_DATASETS: list[PublicDataset] = [
    PublicDataset(
        slug="store-apps",
        description="All apps from app stores with metadata",
    ),
    PublicDataset(
        slug="store-apps-metrics",
        description="App store metrics including ratings and reviews",
    ),
    PublicDataset(
        slug="store-apps-companies",
        description="App company information and associations",
    ),
    PublicDataset(
        slug="live-store-apps",
        description="Live app data currently available in stores",
    ),
    PublicDataset(
        slug="descriptions",
        description="Latest app descriptions and short descriptions",
    ),
]


def get_dataset_by_slug(slug: str) -> PublicDataset | None:
    """Retrieve dataset configuration by slug."""
    for dataset in PUBLIC_DATASETS:
        if dataset.slug == slug:
            return dataset
    return None


def get_export_date_string(date: datetime | None = None) -> str:
    """Return the canonical export date string for monthly files."""
    if date is None:
        date = datetime.today()
    return date.date().strftime(EXPORT_DATE_FORMAT)


def parse_export_date_string(export_date_string: str) -> tuple[int, int, int]:
    """Parse the export date string into numeric year, month, day parts."""
    export_date = datetime.strptime(export_date_string, EXPORT_DATE_FORMAT)
    return export_date.year, export_date.month, export_date.day


def build_object_key(
    dataset_slug: str,
    export_date_string: str,
    filename: str | None = None,
) -> str:
    """
    Build canonical S3 object key for a dataset export.

    Pattern: downloads/{dataset}/year={YYYY}/month={MM}/{filename}

    Args:
        dataset_slug: Canonical dataset slug (e.g., 'store-apps')
        export_date_string: Export date string in YYYY_MM_DD format
        filename: Optional filename override. If None, generates
            "{YYYY}_{MM}_01_{dataset}.tsv.xz"

    Returns:
        Canonical S3 object key path
    """
    year, month, _day = parse_export_date_string(export_date_string)

    if filename is None:
        filename = build_versioned_filename(
            dataset_slug=dataset_slug,
            export_date_string=export_date_string,
        )

    return f"downloads/{dataset_slug}/year={year:04d}/month={month:02d}/{filename}"


def build_versioned_filename(
    dataset_slug: str,
    export_date_string: str,
) -> str:
    """Build default monthly filename: YYYY_MM_DD_dataset.tsv.xz."""
    year, month, day = parse_export_date_string(export_date_string)
    return f"appgoblin_{dataset_slug}_{year:04d}_{month:02d}_{day:02d}.tsv.xz"
