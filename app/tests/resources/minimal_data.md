what's in the minimal_data.pfb file?

`pfb show -i app/tests/resources/minimal_data.pfb | jq .`

returns:

```json
{
  "id": "HG01101_cram",
  "name": "submitted_aligned_reads",
  "object": {
    "file_format": "BAM",
    "error_type": "file_size",
    "file_name": "foo.bam",
    "file_size": 512,
    "file_state": "registered",
    "md5sum": "bdf121aadba028d57808101cb4455fa7",
    "object_id": "dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
    "created_datetime": null,
    "ga4gh_drs_uri": "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa",
    "participant_id": "bbb1234",
    "specimen_id": "spec1111",
    "experimental_strategy": null,
    "study_registration": "example.com/study_registration",
    "study_id": "aaa1234",
    "project_id": "DEV-test",
    "state": "uploading",
    "submitter_id": "HG01101_cram",
    "subject_id": "p1011554-9",
    "updated_datetime": null
  },
  "relations": []
}
```

This file is - exactly - the "Example of minimal PFB" in the [pypfb repository](https://github.com/uc-cdis/pypfb#example-of-minimal-pfb).

Characteristics of this data:
    * 16 non-null properties
    * 3 null properties
    * the `file_size` property contains a number

Translating this PFB to entity operations should result in 16 total operations (16 AddUpdateAttribute)
