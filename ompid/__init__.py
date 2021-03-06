import os
from typing import List

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.params import Depends
from sqlalchemy.orm import Session

from ompid.models import Base, TopioUser, TopioUserCreate, TopioUserORM, \
    TopioAssetType, TopioAssetTypeORM, TopioAsset, TopioAssetORM, \
    TopioAssetCreate, TopioUserQuery


def load_default_configuration():
    with open(os.path.join(os.getcwd(), 'settings.yml')) as yaml_file:
        cfg = yaml.safe_load(yaml_file)

    return cfg


async def get_db():
    from ompid.db import SessionLocal
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()

app = FastAPI()


@app.on_event('startup')
def init_tables():
    import ompid.db
    Base.metadata.create_all(ompid.db.engine)


@app.post('/users/register', response_model=TopioUser)
async def register_user(topio_user: TopioUserCreate, db: Session = Depends(get_db)):
    topio_user_orm = TopioUserORM(
        name=topio_user.name, user_namespace=topio_user.user_namespace)

    db.add(topio_user_orm)
    db.commit()
    db.refresh(topio_user_orm)

    return topio_user_orm


@app.get('/users/{topio_user_id}', response_model=TopioUser)
async def get_user_info(topio_user_id: int, db: Session = Depends(get_db)):
    topio_user_orm = \
        db.query(TopioUserORM).filter(TopioUserORM.id == topio_user_id).first()

    return topio_user_orm


@app.post('/asset_types/register', response_model=TopioAssetType)
async def register_asset_type(
        topio_asset_type: TopioAssetType, db: Session = Depends(get_db)):

    topio_asset_type_orm = TopioAssetTypeORM(
        id=topio_asset_type.id, description=topio_asset_type.description)

    db.add(topio_asset_type_orm)
    db.commit()
    db.refresh(topio_asset_type_orm)

    return topio_asset_type_orm


@app.get('/asset_types/{topio_asset_type_id}', response_model=TopioAssetType)
async def get_asset_namespace_info(
        topio_asset_type_id: str, db: Session = Depends(get_db)):

    topio_asset_type_orm = db\
        .query(TopioAssetTypeORM)\
        .filter(TopioAssetTypeORM.id == topio_asset_type_id)\
        .first()

    return topio_asset_type_orm


@app.get('/asset_types/', response_model=List[TopioAssetType])
async def get_asset_types(db: Session = Depends(get_db)):
    return db.query(TopioAssetTypeORM).all()


@app.post('/assets/register', response_model=TopioAsset)
async def register_asset(topio_asset: TopioAssetCreate, db: Session = Depends(get_db)):
    topio_asset_orm = TopioAssetORM(
        local_id=topio_asset.local_id,
        owner_id=topio_asset.owner_id,
        asset_type=topio_asset.asset_type,
        description=topio_asset.description)

    db.add(topio_asset_orm)
    db.commit()
    db.refresh(topio_asset_orm)

    return topio_asset_orm


@app.get('/assets/topio_id', response_model=str)
async def get_topio_id(
        asset_info: TopioAssetCreate,
        db: Session = Depends(get_db)):
    """
    Returns the topio ID for a given asset identified by
    - the asset owner ID
    - the asset type
    - the asset's local ID (e.g. hdfs://foo/bar, postgresql://user:pw@dbhost/db)

    :param asset_info: basic asset information which has to comprise the asset
        owner ID, the asset type and the asset's local ID
    :param db: database session (will be provided by FastAPI's dependency
        injection mechanism.
    :return: A string containing the topio ID of the respective asset
    """

    if asset_info.local_id is None:
        raise HTTPException(
            status_code=400,
            detail='No asset local ID provided')

    asset = db\
        .query(TopioAssetORM)\
        .filter(TopioAssetORM.owner_id == asset_info.owner_id,
                TopioAssetORM.asset_type == asset_info.asset_type,
                TopioAssetORM.local_id == asset_info.local_id)\
        .first()

    return asset.topio_id


@app.get('/assets/custom_id', response_model=str)
async def get_custom_id(query: dict, db: Session = Depends(get_db)):
    topio_id: str = query.get('topio_id')

    if topio_id is None:
        asset = None
    else:
        asset = db\
            .query(TopioAssetORM)\
            .filter(
                TopioAssetORM.topio_id == topio_id,
                TopioAssetORM.local_id != None)\
            .first()

    if asset is None:
        raise HTTPException(
            404,
            f'No custom ID found for topio ID {topio_id}')

    return asset.local_id


@app.get('/assets/', response_model=List[TopioAsset])
async def get_users_assets(user: TopioUserQuery, db: Session = Depends(get_db)):
    return db\
        .query(TopioAssetORM)\
        .filter(TopioAssetORM.owner_id == user.user_id)\
        .all()
