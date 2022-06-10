import os
from typing import List
import logging
import logging.config

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.params import Depends
from fastapi.responses import JSONResponse
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette import status

from ompid.models import Base, TopioUser, TopioUserCreate, TopioUserORM, \
    TopioAssetType, TopioAssetTypeORM, TopioAsset, TopioAssetORM, \
    TopioAssetCreate, TopioUserQuery

logging.config.fileConfig(os.getenv('LOGGING_FILE_CONFIG', './logging.conf'))
logger = logging.getLogger(__name__)


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
    logger.info("Initialized database")

@app.post('/users/register', response_model=TopioUser, responses={201: {"model": TopioUser}})
async def register_user(topio_user: TopioUserCreate, db: Session = Depends(get_db)):

    topio_user_orm = (
        db
        .query(TopioUserORM)
        .filter(and_(TopioUserORM.name == topio_user.name, TopioUserORM.user_namespace == topio_user.user_namespace))
        .first()
    )

    if topio_user_orm is not None:
        return topio_user_orm

    topio_user_orm = TopioUserORM(
        name=topio_user.name, user_namespace=topio_user.user_namespace)

    try:
        db.add(topio_user_orm)
        db.commit()
        db.refresh(topio_user_orm)
    except Exception as e:
        # interfering here to really log errors which are otherwise not reported
        # by FastAPI
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    topio_user_json = jsonable_encoder(topio_user_orm)
    return JSONResponse(
        status_code=status.HTTP_201_CREATED, content=topio_user_json)


@app.get('/users/{topio_user_id}', response_model=TopioUser)
async def get_user_info(topio_user_id: int, db: Session = Depends(get_db)):
    try:
        topio_user_orm = \
            db.query(TopioUserORM).filter(TopioUserORM.id == topio_user_id).first()
    except Exception as e:
        # interfering here to really log errors which are otherwise not reported
        # by FastAPI
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    if topio_user_orm is None:
        err_msg = f'User with user ID {topio_user_id} could not be found'
        logger.warning(err_msg)

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=err_msg)

    return topio_user_orm


@app.post('/asset_types/register', response_model=TopioAssetType, responses={201: {"model": TopioAssetType}})
async def register_asset_type(
        topio_asset_type: TopioAssetType, db: Session = Depends(get_db)):

    try:
        topio_asset_type_orm = (
            db
            .query(TopioAssetTypeORM)
            .filter(and_(TopioAssetTypeORM.id == topio_asset_type.id, TopioAssetTypeORM.description == topio_asset_type.description))
            .first()
        )
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    if topio_asset_type_orm is not None:
        return topio_asset_type_orm

    topio_asset_type_orm = TopioAssetTypeORM(
        id=topio_asset_type.id, description=topio_asset_type.description)

    try:
        db.add(topio_asset_type_orm)
        db.commit()
        db.refresh(topio_asset_type_orm)
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    topio_asset_type_json = jsonable_encoder(topio_asset_type_orm)
    return JSONResponse(status_code=201, content=topio_asset_type_json)


@app.get('/asset_types/{topio_asset_type_id}', response_model=TopioAssetType)
async def get_asset_namespace_info(
        topio_asset_type_id: str, db: Session = Depends(get_db)):

    try:
        topio_asset_type_orm = db\
            .query(TopioAssetTypeORM)\
            .filter(TopioAssetTypeORM.id == topio_asset_type_id)\
            .first()
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    if topio_asset_type_orm is not None:
        return topio_asset_type_orm

    else:
        err_msg = f'Asset type with ID {topio_asset_type_id} not found'
        logger.warning(err_msg)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=err_msg)


@app.get('/asset_types/', response_model=List[TopioAssetType])
async def get_asset_types(db: Session = Depends(get_db)):
    try:
        res = db.query(TopioAssetTypeORM).all()
    except Exception as e:
        logger.error(e)

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    return res


@app.post('/assets/register', response_model=TopioAsset)
async def register_asset(topio_asset: TopioAssetCreate, db: Session = Depends(get_db)):
    topio_asset_orm = TopioAssetORM(
        local_id=topio_asset.local_id,
        owner_id=topio_asset.owner_id,
        asset_type=topio_asset.asset_type,
        description=topio_asset.description)

    try:
        db.add(topio_asset_orm)
        db.commit()
        db.refresh(topio_asset_orm)
    except IntegrityError as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e))
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    return topio_asset_orm


@app.get('/assets/topio_id', response_model=str, responses={404: {"model": str}})
async def get_topio_id(
        owner_id: int,
        asset_type: str, 
        local_id: str,
        db: Session = Depends(get_db)):
    """
    Returns the topio ID for a given asset identified by
    - the asset owner ID
    - the asset type
    - the asset's local ID (e.g. hdfs://foo/bar, postgresql://user:pw@dbhost/db)

    :param owner_id: the asset owner ID
    :param asset_type: the asset type
    :param local_id: the asset's local ID
    :param db: database session (will be provided by FastAPI's dependency
        injection mechanism.
    :return: A string containing the topio ID of the respective asset
    """

    try:
        asset = db\
            .query(TopioAssetORM)\
            .filter(TopioAssetORM.owner_id == owner_id,
                    TopioAssetORM.asset_type == asset_type,
                    TopioAssetORM.local_id == local_id)\
            .first()
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    if asset is None:
        err_msg = 'No topio ID found for the given parameters'
        logger.warning(err_msg)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=err_msg)

    return asset.topio_id


@app.get('/assets/custom_id', response_model=str)
async def get_custom_id(query: dict, db: Session = Depends(get_db)):
    topio_id: str = query.get('topio_id')

    if topio_id is None:
        asset = None
    else:
        try:
            asset = db\
                .query(TopioAssetORM)\
                .filter(
                    TopioAssetORM.topio_id == topio_id,
                    TopioAssetORM.local_id != None)\
                .first()
        except Exception as e:
            logger.error(e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e))

    if asset is None:
        err_msg = f'No custom ID found for topio ID {topio_id}'
        logger.warning(err_msg)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=err_msg)

    return asset.local_id


@app.get('/assets/', response_model=List[TopioAsset])
async def get_users_assets(user: TopioUserQuery, db: Session = Depends(get_db)):
    try:
        assets = db\
            .query(TopioAssetORM)\
            .filter(TopioAssetORM.owner_id == user.user_id)\
            .all()
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))

    return assets
