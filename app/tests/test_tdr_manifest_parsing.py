# import flask.testing
# import pytest
# import os

import copy
import json
from collections import namedtuple

from urllib.parse import urlparse

def objDecoder(dict):
    return namedtuple('X', dict.keys())(*dict.values())



def test_url_parsing():
    parsedurl = urlparse("gs://datarepo-tools-83c20e98-snapshot-export-bucket/4WKdTpfwTmOevbNouz171g/manifest.json")
    assert parsedurl.scheme == "gs"
    assert parsedurl.hostname == "datarepo-tools-83c20e98-snapshot-export-bucket"
    assert parsedurl.path == "/4WKdTpfwTmOevbNouz171g/manifest.json"

def test_raw_dicts():
    # read the file into a raw dict
    jso = json.load(open('./app/tests/response_1638551384572.json'))

    tablekeys = []
    for t in jso["tables"]:
        pk = "datarepo_row_id"
        if (t["primaryKey"] is not None):
            pk = t["primaryKey"]
        tablekeys.append(f"Table {t['name']} has primary key {pk}")

    assert len(tablekeys) == len(jso["tables"])
    
    relationships = []
    for r in jso["relationships"]:
        f = r["from"]
        t = r["to"]
        relationships.append(f"{f['table']}.{f['column']} -> {t['table']}.{t['column']}")
    
    assert len(relationships) == len(jso["relationships"])

    assert jso["id"] == "8bf100c0-7ac3-4860-80ca-28093f4adb61"
    assert jso["name"] == "hca_dev_90bd693340c048d48d76778c103bf545__20210827_20211110"
    

# def test_client_lib_models():
#     # the SnapshotModel object contains a key "relationships", and inside that is a key "from"
#     # but "from" is a reserved keyword in Python, so we cannot blindly parse from a json string
#     # into that object. The relationships key needs special attention.

#     # read the file into a raw dict
#     jso = json.load(open('./app/tests/response_1638551384572.json'))
#     # remove relationships from the dict, but save its value
#     relationshipsDict = jso.pop("relationships")
#     jso["relationships"] = []
#     # now parse the dict, minus relationships, into a SnapshotModel
#     snapshotObj = json.loads(json.dumps(jso), object_hook=objDecoder)
    
#     # manually build the relationships object
#     relationships = []
#     for r in relationshipsDict:
#         t = r["to"]
#         toModel = RelationshipTermModel(table=t["table"], column=t["column"])
#         f = r["from"]
#         fromModel = RelationshipTermModel(table=f["table"], column=f["column"])
#         x = RelationshipModel(name=r["name"], _from = fromModel, to = toModel)
#         relationships.append(x)

#     snapshotModel = SnapshotModel(id=snapshotObj.id,
#                                     name=snapshotObj.name,
#                                     description=snapshotObj.description,
#                                     created_date=snapshotObj.createdDate,
#                                     tables=snapshotObj.tables,
#                                     relationships=relationships,
#                                     profile_id=snapshotObj.profileId,
#                                     data_project=snapshotObj.dataProject,
#                                     access_information=None,
#                                     local_vars_configuration=None)
        
#     assert len(snapshotModel.tables) == 27
#     assert snapshotModel.name == "hca_dev_90bd693340c048d48d76778c103bf545__20210827_20211110"
#     assert len(snapshotModel.relationships) == 1

#     tablekeys = []
#     for t in snapshotModel.tables:
#         pk = "datarepo_row_id"
#         if (t.primaryKey is not None):
#             pk = t.primaryKey
#         tablekeys.append(f"Table {t.name} has primary key {pk}")
    
#     assert len(tablekeys) == len(snapshotModel.tables)


