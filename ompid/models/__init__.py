import re
from typing import Optional, Tuple

import pydantic
from pydantic import validator
from sqlalchemy import Column, ForeignKey, func
from sqlalchemy import select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import column_property, ColumnProperty
from sqlalchemy.sql.sqltypes import Integer, String

Base = declarative_base()

# ------------ topio ID pattern related constants and methods -----------------
TOPIO_ID_SCHEMA = 'topio.{owner_namespace}.{asset_id}.{asset_type}'


def topio_id_to_parts(topio_id: str) -> Tuple[int, str, str]:
    _, owner_ns, asset_id, asset_type = topio_id.split('.')
    asset_id = int(asset_id)
    return asset_id, asset_type, owner_ns


def build_topio_id_column_property(
        owner_namespace: ColumnProperty,
        asset_id: int,
        asset_type: str):

    return 'topio.' + owner_namespace.expression + '.' + \
        func.cast(asset_id, String) + '.' + asset_type
# -----------------------------------------------------------------------------


class TopioUserORM(Base):
    """
    A top.io user is a person or organization that uses the top.io portal.
    A user is assumed to have a name and a user namespace which will be used to
    generate asset IDs for a user. Moreover, the user will get a top.io user ID
    during registration.
    """
    __tablename__ = 'topio_user'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    user_namespace = Column(String, unique=True)


class TopioUserQuery(pydantic.BaseModel):
    user_id: int


class TopioUserCreate(pydantic.BaseModel):
    name: str
    user_namespace: str

    @validator('user_namespace')
    def _validate_user_namespace(cls, ns):
        if re.search(r'\s', ns):
            raise ValueError('user namespace must not contain whitespace')
        return ns


class TopioUser(TopioUserCreate):
    id: int

    class Config:
        orm_mode = True


class TopioAssetTypeORM(Base):
    """
    A top.io asset type is a category or class summarizing a set of similar
    assets. The main proposed categories/types so far are

      - dataset
      - service

    In terms of PID creation they are mainly used as second-level namespace
    identifier used after the top level user namespace, e.g.

      user23.dataset.345

    Assets types are registered globally s.t. they are available to all users.
    """
    __tablename__ = 'topio_asset_type'

    id = Column(String, primary_key=True)
    description = Column(String)


class TopioAssetType(pydantic.BaseModel):
    id: str
    description: Optional[str] = None

    @validator('id')
    def _validate_asset_type_id(cls, asset_type_id):
        if re.search(r'\s', asset_type_id):
            raise ValueError(
                'Asset type identifier must not contain whitespace')

        return asset_type_id

    class Config:
        orm_mode = True


class TopioAssetORM(Base):
    """
    A top.io asset is any artifact deployed on the top.io platform, be it a
    dataset, a service etc. A top.io asset has an asset type which is one of the
    TopioAssetType entries registered via the PID service. Moreover it may have
    some kind of identifier or name which we treat as a *local* identifier valid
    in a certain realm, e.g. the primary key in a database table, a file name,
    or an HDFS URI. What the PID service provides during registration of such an
    asset is a *global* identifier which is valid everywhere in the top.io
    platform. Further it provides means to translate between local and global
    identifiers.
    """
    __tablename__ = 'topio_asset'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    local_id = Column(String)
    owner_id = Column(Integer, ForeignKey('topio_user.id'))
    asset_type = Column(String, ForeignKey('topio_asset_type.id'))
    description = Column(String)

    owner_namespace = column_property(
        select([TopioUserORM.user_namespace])
        .where(TopioUserORM.id == owner_id).as_scalar())

    topio_id = column_property(
        build_topio_id_column_property(owner_namespace, id, asset_type))


class TopioAssetCreate(pydantic.BaseModel):
    local_id: Optional[str] = None
    owner_id: int
    asset_type: str
    description: Optional[str] = None


class TopioAsset(TopioAssetCreate):
    id: int
    topio_id: str

    class Config:
        orm_mode = True
