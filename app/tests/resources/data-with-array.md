what's in the data-with-array.pfb file?

`pfb show -i app/tests/resources/data-with-array.pfb | jq .`

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
    "file_state": [
      "00000000-0000-0000-0000-000000000000",
      "11111111-1111-1111-1111-111111111111",
      "22222222-2222-2222-2222-222222222222"
    ],
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

This file is based off the "Example of minimal PFB" in the [pypfb repository](https://github.com/uc-cdis/pypfb#example-of-minimal-pfb), but has been modified to include an array of values in the `file_state` property.

Characteristics of this data:
    * 16 non-null properties
    * 3 null properties
    * the `file_size` property contains a number
    * the `file_state` property is an arraywith 3 values

Translating this PFB to entity operations should result in 20 total operations (15 AddUpdateAttribute, 1 RemoveAttribute, 1 CreateAttributeValueList, 3 AddListMember)
