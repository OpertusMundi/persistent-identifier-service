import json

from pytest_postgresql.compat import connection, cursor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

import ompid
import ompid.db
from ompid import app, Base
from ompid.models import TOPIO_ID_SCHEMA


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


def test_users_info(postgresql: connection):
    client = _init_test_client(postgresql)

    user_name = 'User ABC'
    user_namespace = 'abc'
    response = client.post(
        '/users/register',
        json={'name': user_name, 'user_namespace': user_namespace})
    user_id: int = json.loads(response.content)['id']

    response = client.get(
        f'/users/{user_id}',
        json={'topio_user_id': user_id})

    assert response.status_code == 200

    # {"name":"User ABC","user_namespace":"abc","id":1}
    user_info_data = json.loads(response.content)

    assert user_info_data['name'] == user_name
    assert user_info_data['user_namespace'] == user_namespace
    assert user_info_data['id'] == user_id


def test_asset_types_register(postgresql: connection):
    client = _init_test_client(postgresql)

    # normal asset type registration
    asset_type_id = 'file'
    asset_type_description = 'Data assets provided as downloadable file'

    response = client.post(
        '/asset_types/register',
        json={'id': asset_type_id, 'description': asset_type_description}
    )

    assert response.status_code == 200

    cur: cursor = postgresql.cursor()
    cur.execute(
        f'SELECT * FROM topio_asset_type WHERE id=%s;',
        (asset_type_id,))
    results = cur.fetchall()
    cur.close()
    assert len(results) == 1
    assert results[0][1] == asset_type_description

    # registration of asset type with broken asset type ID (contains spaces)
    asset_type_id = 'this is broken'
    asset_type_description = \
        'This is a broken asset type with spaces in its identifier string'

    response = client.post(
        '/asset_types/register',
        json={'id': asset_type_id, 'description': asset_type_description}
    )

    assert response.status_code == 422

    cur: cursor = postgresql.cursor()
    cur.execute(
        f'SELECT * FROM topio_asset_type WHERE id=%s;',
        (asset_type_id,))
    results = cur.fetchall()
    cur.close()
    assert len(results) == 0


def test_asset_types_info(postgresql: connection):
    client = _init_test_client(postgresql)

    asset_type_id = 'file'
    asset_type_description = 'Data assets provided as downloadable file'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_id, 'description': asset_type_description}
    )

    response = client.get(
        f'/asset_types/{asset_type_id}',
        json={'id': asset_type_id}
    )

    assert response.status_code == 200

    asset_info_data = json.loads(response.content)

    assert asset_info_data['id'] == asset_type_id
    assert asset_info_data['description'] == asset_type_description


def test_asset_types_list(postgresql: connection):
    client = _init_test_client(postgresql)

    asset_type_1_id = 'file'
    asset_type_1_description = 'Data assets provided as downloadable file'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_1_id, 'description': asset_type_1_description}
    )

    asset_type_2_id = 'api'
    asset_type_2_description = \
        'Data that is provided via a well defined application programming ' \
        'interface'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_2_id, 'description': asset_type_2_description}
    )

    asset_type_3_id = 'stream'
    asset_type_3_description = \
        'Data that is constantly updated and thus provided as a series of ' \
        'data values'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_3_id, 'description': asset_type_3_description}
    )

    response = client.get('/asset_types/')

    assert response.status_code == 200

    assets_list = json.loads(response.content)

    assert len(assets_list) == 3

    # [{
    #       'id': 'file',
    #       'description': 'Data assets provided as downloadable file'
    #  },
    #  {
    #       'id': 'api',
    #       'description': 'Data that is provided via a well defined ...'
    #  },
    #  {
    #       'id': 'stream',
    #       'description': 'Data that is constantly updated and thus ...'
    #  }
    # ]
    assert assets_list[0]['id'] == asset_type_1_id
    assert assets_list[0]['description'] == asset_type_1_description

    assert assets_list[1]['id'] == asset_type_2_id
    assert assets_list[1]['description'] == asset_type_2_description

    assert assets_list[2]['id'] == asset_type_3_id
    assert assets_list[2]['description'] == asset_type_3_description


def test_assets_register(postgresql: connection):
    client = _init_test_client(postgresql)

    # register asset owner
    owner_name = 'User ABC'
    owner_namespace = 'abc'
    response = client.post(
        '/users/register',
        json={'name': owner_name, 'user_namespace': owner_namespace})
    owner_id = json.loads(response.content)['id']

    # register asset type
    asset_type_id = 'file'
    asset_type_description = 'Data assets provided as downloadable file'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_id, 'description': asset_type_description})

    asset_1_local_id = 'hdfs://foo.bar.ttl'
    asset_1_description = 'A Turtle HDFS file'

    response = client.post(
        '/assets/register',
        json={
            'local_id': asset_1_local_id,
            'owner_id': owner_id,
            'asset_type': asset_type_id,
            'description': asset_1_description})

    assert response.status_code == 200

    # {
    #   "local_id":"hdfs://foo.bar.ttl",
    #   "owner_id":1,
    #   "asset_type":"file",
    #   "description":"A Turtle HDFS file",
    #   "topio_id":"topio.abc.1.file"
    # }
    asset_1_topio_id = json.loads(response.content)['topio_id']
    asset_1_id = json.loads(response.content)['id']

    assert asset_1_topio_id == \
        TOPIO_ID_SCHEMA.format(**{
            'owner_namespace': owner_namespace,
            'asset_id': asset_1_id,
            'asset_type': asset_type_id})

    cur: cursor = postgresql.cursor()
    cur.execute(
        f'SELECT * FROM topio_asset WHERE id=%s;',
        (asset_1_id,))
    results = cur.fetchall()
    cur.close()

    assert len(results) == 1

    # [(1, 'hdfs://foo.bar.ttl', 1, 'file', 'A Turtle HDFS file')]
    assert results[0][0] == asset_1_id
    assert results[0][1] == asset_1_local_id
    assert results[0][2] == owner_id
    assert results[0][3] == asset_type_id
    assert results[0][4] == asset_1_description

    # asset without local ID and description
    response = client.post(
        '/assets/register',
        json={'owner_id': owner_id, 'asset_type': asset_type_id})

    assert response.status_code == 200

    # {
    #   "local_id":null,
    #   "owner_id":1,
    #   "asset_type":"file",
    #   "description":null,
    #   "id":2,
    #   "topio_id":"topio.abc.2.file"
    # }
    asset_2_topio_id = json.loads(response.content)['topio_id']
    asset_2_id = json.loads(response.content)['id']

    assert asset_2_topio_id == \
        TOPIO_ID_SCHEMA.format(**{
            'owner_namespace': owner_namespace,
            'asset_id': asset_2_id,
            'asset_type': asset_type_id})

    cur: cursor = postgresql.cursor()
    cur.execute(
        f'SELECT * FROM topio_asset WHERE id=%s;',
        (asset_2_id,))
    results = cur.fetchall()
    cur.close()

    assert len(results) == 1

    # [(2, None, 1, 'file', None)]
    assert results[0][0] == asset_2_id
    assert results[0][1] is None
    assert results[0][2] == owner_id
    assert results[0][3] == asset_type_id
    assert results[0][4] is None


def test_assets_topio_id(postgresql: connection):
    client = _init_test_client(postgresql)

    # register asset owner
    owner_name = 'User ABC'
    owner_namespace = 'abc'
    response = client.post(
        '/users/register',
        json={'name': owner_name, 'user_namespace': owner_namespace})
    owner_id = json.loads(response.content)['id']

    # register asset type
    asset_type_id = 'file'
    asset_type_description = 'Data assets provided as downloadable file'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_id, 'description': asset_type_description})

    # This asset should be ignored by the get_topio_id method as it does not
    # have a local ID. So local-ID-to-topio-ID doesn't make sense here.
    client.post(
        '/assets/register',
        json={'owner_id': owner_id, 'asset_type': asset_type_id})

    # Asset with proper local ID which should be considered by the get_topio_id
    # method
    asset_2_local_id = 'hdfs://foo.bar.ttl'
    asset_2_description = 'A Turtle HDFS file'
    response = client.post(
        '/assets/register',
        json={
            'owner_id': owner_id,
            'asset_type': asset_type_id,
            'local_id': asset_2_local_id,
            'description': asset_2_description})

    asset_2_id = json.loads(response.content)['id']
    del response

    # Calling /assets/topio_id with an undefined local ID should always return
    # a client error
    response = client.get(
        '/assets/topio_id',
        json={
            'owner_id': owner_id,
            'asset_type': asset_type_id})

    assert response.status_code == 400

    # Calling /assets/topio_id providing a local ID should return the correct
    # result
    response = client.get(
        '/assets/topio_id',
        json={
            'owner_id': owner_id,
            'asset_type': asset_type_id,
            'local_id': asset_2_local_id})

    assert response.status_code == 200

    returned_topio_id = json.loads(response.content)

    assert returned_topio_id == TOPIO_ID_SCHEMA.format(**{
        'owner_namespace': owner_namespace,
        'asset_id': asset_2_id,
        'asset_type': asset_type_id})


def test_assets_custom_id(postgresql: connection):
    client = _init_test_client(postgresql)

    # register asset owner
    owner_name = 'User ABC'
    owner_namespace = 'abc'
    response = client.post(
        '/users/register',
        json={'name': owner_name, 'user_namespace': owner_namespace})
    owner_id = json.loads(response.content)['id']

    # register asset type
    asset_type_id = 'file'
    asset_type_description = 'Data assets provided as downloadable file'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_id, 'description': asset_type_description})

    # Asset without a local ID
    response = client.post(
        '/assets/register',
        json={'owner_id': owner_id, 'asset_type': asset_type_id})

    asset_1_id = json.loads(response.content)['id']
    asset_1_topio_id = TOPIO_ID_SCHEMA.format(**{
        'owner_namespace': owner_namespace,
        'asset_id': asset_1_id,
        'asset_type': asset_type_id})

    del response

    # Asset with proper local ID
    asset_2_local_id = 'hdfs://foo.bar.ttl'
    asset_2_description = 'A Turtle HDFS file'
    response = client.post(
        '/assets/register',
        json={
            'owner_id': owner_id,
            'asset_type': asset_type_id,
            'local_id': asset_2_local_id,
            'description': asset_2_description})

    asset_2_id = json.loads(response.content)['id']

    asset_2_topio_id = TOPIO_ID_SCHEMA.format(**{
        'owner_namespace': owner_namespace,
        'asset_id': asset_2_id,
        'asset_type': asset_type_id})

    del response

    response = client.get(
        '/assets/custom_id',
        json={'topio_id': asset_1_topio_id})

    assert response.status_code == 404  # as there is no local ID for asset 1

    del response

    response = client.get(
        '/assets/custom_id',
        json={'topio_id': asset_2_topio_id})

    assert response.status_code == 200

    returned_local_id = json.loads(response.content)
    assert returned_local_id == asset_2_local_id


def test_assets_list(postgresql: connection):
    client = _init_test_client(postgresql)

    # register asset owner
    owner_name = 'User ABC'
    owner_namespace = 'abc'
    response = client.post(
        '/users/register',
        json={'name': owner_name, 'user_namespace': owner_namespace})
    owner_id = json.loads(response.content)['id']

    # register asset types
    asset_type_1_id = 'file'
    asset_type_1_description = 'Data assets provided as downloadable file'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_1_id, 'description': asset_type_1_description})

    asset_type_2_id = 'api'
    asset_type_2_description = \
        'Data that is provided via a well defined application programming ' \
        'interface'

    client.post(
        '/asset_types/register',
        json={'id': asset_type_2_id, 'description': asset_type_2_description})

    # register assets
    response = client.post(
        '/assets/register',
        json={'owner_id': owner_id, 'asset_type': asset_type_1_id})

    asset_1_id = json.loads(response.content)['id']
    asset_1_topio_id = TOPIO_ID_SCHEMA.format(**{
        'owner_namespace': owner_namespace,
        'asset_id': asset_1_id,
        'asset_type': asset_type_1_id})

    asset_2_local_id = 'hdfs://foo.bar.ttl'
    asset_2_description = 'A Turtle HDFS file'
    response = client.post(
        '/assets/register',
        json={
            'owner_id': owner_id,
            'asset_type': asset_type_1_id,
            'local_id': asset_2_local_id,
            'description': asset_2_description})

    asset_2_id = json.loads(response.content)['id']
    asset_2_topio_id = TOPIO_ID_SCHEMA.format(**{
        'owner_namespace': owner_namespace,
        'asset_id': asset_2_id,
        'asset_type': asset_type_1_id})

    asset_3_local_id = 'http://topio.market:7777/api'
    response = client.post(
        '/assets/register',
        json={
            'owner_id': owner_id,
            'asset_type': asset_type_2_id,
            'local_id': asset_3_local_id})

    asset_3_id = json.loads(response.content)['id']
    asset_3_topio_id = TOPIO_ID_SCHEMA.format(**{
        'owner_namespace': owner_namespace,
        'asset_id': asset_3_id,
        'asset_type': asset_type_2_id})

    response = client.get(
        '/assets/',
        json={'user_id': owner_id})

    assert response.status_code == 200

    # [
    #   {
    #     "local_id":null,
    #     "owner_id":1,
    #     "asset_type":"file",
    #     "description":null,
    #     "id":1,
    #     "topio_id":"topio.abc.1.file"
    #   },
    #   {
    #     "local_id":"hdfs://foo.bar.ttl",
    #     "owner_id":1,
    #     "asset_type":"file",
    #     "description":"A Turtle HDFS file",
    #     "id":2,
    #     "topio_id":"topio.abc.2.file"
    #   },
    #   {
    #     "local_id":"http://topio.market:7777/api",
    #     "owner_id":1,
    #     "asset_type":"api",
    #     "description":null,
    #     "id":3,
    #     "topio_id":"topio.abc.3.api"
    #   }
    # ]
    results = json.loads(response.content)

    # asset 1 info
    tmp_res = list(filter(lambda d: d['id'] == asset_1_id, results))
    assert len(tmp_res) == 1
    tmp_res = tmp_res[0]
    assert tmp_res['local_id'] is None
    assert tmp_res['owner_id'] == owner_id
    assert tmp_res['asset_type'] == asset_type_1_id
    assert tmp_res['description'] is None
    assert tmp_res['topio_id'] == asset_1_topio_id

    # asset 2 info
    tmp_res = list(filter(lambda d: d['id'] == asset_2_id, results))
    assert len(tmp_res) == 1
    tmp_res = tmp_res[0]
    assert tmp_res['local_id'] == asset_2_local_id
    assert tmp_res['owner_id'] == owner_id
    assert tmp_res['asset_type'] == asset_type_1_id
    assert tmp_res['description'] == asset_2_description
    assert tmp_res['topio_id'] == asset_2_topio_id

    # asset 3 info
    tmp_res = list(filter(lambda d: d['id'] == asset_3_id, results))
    assert len(tmp_res) == 1
    tmp_res = tmp_res[0]
    assert tmp_res['local_id'] == asset_3_local_id
    assert tmp_res['owner_id'] == owner_id
    assert tmp_res['asset_type'] == asset_type_2_id
    assert tmp_res['description'] is None
    assert tmp_res['topio_id'] == asset_3_topio_id
