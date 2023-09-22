import os
import re
from typing import List


def url_pattern_for_host(hostname: str) -> re.Pattern:
    """Returns a pattern matching URLs for files on the given hostname and its subdomains."""
    return re.compile(f"^https://(.+)?{re.escape(hostname)}/")

def url_patterns_for_s3_bucket(bucket_name: str) -> List[re.Pattern]:
    """
    Returns patterns matching URLs for files in the given S3 bucket.
    
    This returns multiple patterns because S3 supports multiple URL formats.
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
    """
    virtual_host_based_global_endpoint_url = f"https://{bucket_name}.s3.amazonaws.com/"
    path_based_global_endpoint_url = f"https://s3.amazonaws.com/{bucket_name}/"
    return [
        re.compile("^" + re.escape(virtual_host_based_global_endpoint_url)),
        re.compile("^" + re.escape(path_based_global_endpoint_url)),
    ]

PROTECTED_URL_PATTERNS: List[re.Pattern] = [
    # AnVIL production
    url_pattern_for_host("service.prod.anvil.gi.ucsc.edu"),
    *url_patterns_for_s3_bucket("edu-ucsc-gi-platform-anvil-prod-storage-anvilprod.us-east-1"),
    # AnVIL development
    url_pattern_for_host("service.anvil.gi.ucsc.edu"),
    # BioData Catalyst
    url_pattern_for_host("gen3.biodatacatalyst.nhlbi.nih.gov"),
    *url_patterns_for_s3_bucket("gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export"),
    *url_patterns_for_s3_bucket("gen3-theanvil-io-pfb-export"),
]

def get_restricted_url_patterns() -> List[re.Pattern]:
    """
    By default import service allows imports from any S3 or GCS bucket or Azure blob storage container.
    This allows denying imports from specific S3 buckets. From the IMPORT_RESTRICTED_SOURCES environment
    variable, it get a list of patterns matching URLs for imports from those sources.
    
    IMPORT_RESTRICTED_SOURCES should be a comma separated list where each item is an s3://bucket URL.
    """
    restricted_sources_config = os.getenv("IMPORT_RESTRICTED_SOURCES")
    if not restricted_sources_config:
        return []
    
    restricted_url_patterns: List[re.Pattern] = []
    restricted_sources = [s.strip() for s in restricted_sources_config.split(",")] 
    for source in restricted_sources:
        if source.startswith("s3://"):
            bucket_name = source[5:]
            restricted_url_patterns.extend(url_patterns_for_s3_bucket(bucket_name))

    return restricted_url_patterns


RESTRICTED_URL_PATTERNS: List[re.Pattern] = get_restricted_url_patterns()

def is_restricted_import(import_url: str) -> bool:
    """Whether or not an import URL points to a file from a restricted source."""
    return any(pattern.match(import_url) for pattern in RESTRICTED_URL_PATTERNS)
