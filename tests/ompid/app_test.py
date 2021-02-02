import json

from pytest_postgresql.compat import connection, cursor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

import ompid
import ompid.db
from ompid import app, Base


def _init_test_client(postgresql: connection) -> TestClient:
    mock_engine = create_engine(
        name_or_url='postgresql://',
        connect_args=postgresql.get_dsn_parameters())

    async def get_mock_db():
        MockSessionLocal = \
            sessionmaker(autocommit=False, autoflush=False, bind=mock_engine)
        db = MockSessionLocal()

        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[ompid.get_db] = get_mock_db
    Base.metadata.create_all(mock_engine)

    return TestClient(app)


def test_users_register(postgresql: connection):
    client = _init_test_client(postgresql)

    # normal user -----------------------------------------
    user_name = 'User ABC'
    user_namespace = 'abc'
    response = client.post(
        '/users/register',
        json={'name': user_name, 'user_namespace': user_namespace})

    # no errors were raised
    assert response.status_code == 200

    # user data can be found in the database
    user_id: int = json.loads(response.content)['id']
    cur: cursor = postgresql.cursor()
    cur.execute(
        f'SELECT * FROM topio_user '
        f'WHERE id=%s AND name=%s AND user_namespace=%s;',
        (user_id, user_name, user_namespace))
    results = cur.fetchall()
    cur.close()
    assert len(results) == 1

    # user with broken namespace (contains whitespace) ----
    user_name = 'User DEF'
    user_namespace = 'this is broken'
    response = client.post(
        '/users/register',
        json={'name': user_name, 'user_namespace': user_namespace})

    # should cause a client error 422 (Unprocessable Entity)...
    assert response.status_code == 422

    # ...and it should be a value error
    error_type = json.loads(response.content)['detail'][0]['type']
    assert error_type == 'value_error'

    # ...and no DB write should have happened
    cur: cursor = postgresql.cursor()
    cur.execute(
        f'SELECT * FROM topio_user '
        f'WHERE id=%s AND name=%s AND user_namespace=%s;',
        (user_id, user_name, user_namespace))
    results = cur.fetchall()
    cur.close()
    assert len(results) == 0
