from dataclasses import dataclass
from typing import Sequence, Union

from app.external import JSON


@dataclass
class EntityReference:
     entityName: str
     entityType: str


# alias for attribute values
AttributeValue = Union[bool, str, int, float, EntityReference, JSON]

# base class for operations
@dataclass
class AttributeOperation:
    pass

@dataclass
class AddUpdateAttribute(AttributeOperation):
    attributeName: str
    addUpdateAttribute: AttributeValue
    op: str = 'AddUpdateAttribute'

@dataclass
class AddListMember(AttributeOperation):
    attributeListName: str
    newMember: AttributeValue
    op: str = 'AddListMember'

@dataclass
class RemoveAttribute(AttributeOperation):
    attributeName: str
    op: str = 'RemoveAttribute'

@dataclass
class CreateAttributeValueList(AttributeOperation):
    attributeName: str
    op: str = 'CreateAttributeValueList'

@dataclass
class CreateAttributeEntityReferenceList(AttributeOperation):
    attributeName: str
    op: str = 'CreateAttributeEntityReferenceList'

@dataclass
class Entity:
    name: str
    entityType: str
    operations: Sequence[AttributeOperation]
