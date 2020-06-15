import pydantic
from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import Integer, String


Base = declarative_base()


class OMServiceORM(Base):
    __tablename__ = 'op_ext_service'

    id = Column(Integer, primary_key=True, index=True)
    # TODO: PW: Maybe it makes more sense to store the API description instead
    address = Column(String)


class OMServiceCreate(pydantic.BaseModel):
    address: str


class OMService(OMServiceCreate):
    id: int

    class Config:
        orm_mode = True

