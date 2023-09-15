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
    # AnVIL development
    url_pattern_for_host("service.anvil.gi.ucsc.edu"),
    # BioData Catalyst
    url_pattern_for_host("gen3.biodatacatalyst.nhlbi.nih.gov"),
    *url_patterns_for_s3_bucket("gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export"),
    *url_patterns_for_s3_bucket("gen3-theanvil-io-pfb-export"),
]
