from dataclasses import dataclass
from typing import Any, Sequence

@dataclass
class EntityReference:
     entityName: str
     entityType: str

# base class for operations
@dataclass
class AttributeOperation:
    pass

@dataclass
class AddUpdateAttribute(AttributeOperation):
    attributeName: str
    addUpdateAttribute: Any
    op: str = 'AddUpdateAttribute'

@dataclass
class AddListMember(AttributeOperation):
    attributeListName: str
    newMember: Any
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