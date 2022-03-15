from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import ompid


def build_postgresql_url(settings):
    pg_settings = settings['postgresql']
    return f'postgresql://' \
        f'{pg_settings["user"]}:{pg_settings["password"]}' \
        f'@{pg_settings["host"]}:{pg_settings["port"]}' \
        f'/{pg_settings["db"]}'


engine = create_engine(
    build_postgresql_url(ompid.load_default_configuration()),
    echo=True,
    pool_pre_ping=True)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

