import os

import yaml
from fastapi import FastAPI
from fastapi.params import Depends
from sqlalchemy.orm import Session

from ompid.models import OMService, OMServiceCreate, OMServiceORM, Base


def load_default_configuration():
    with open(os.path.join(os.getcwd(), 'settings.yml')) as yaml_file:
        cfg = yaml.safe_load(yaml_file)

    return cfg


def get_db():
    from ompid.db import SessionLocal
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_tables():
    import ompid.db
    Base.metadata.create_all(ompid.db.engine)


app = FastAPI()
init_tables()


@app.post('/services/register', response_model=OMService)
def register_service(
        om_service: OMServiceCreate, db: Session = Depends(get_db)):

    om_service_orm = OMServiceORM(address=om_service.address)
    db.add(om_service_orm)
    db.commit()
    db.refresh(om_service_orm)

    return om_service_orm


@app.get('/services/{service_id}', response_model=OMService)
def get_service_info(om_service_id: int, db: Session = Depends(get_db)):
    om_service_orm = \
        db.query(OMServiceORM).filter(OMServiceORM.id == om_service_id).first()

    return om_service_orm

