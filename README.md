# Persistent Identifier Service

The Persistent Identifier Service is a REST service running on [uvicorn](https://github.com/encode/uvicorn). To get all dependencies run the following command inside the `persistent-identifier-service/`directory:

```bash
$ pip install .
```

For permanent storage the Persistent Identifier currently uses PostgreSQL. To configure the database access copy the settings template file (`settings.yml.template`) to `settings.yml` and make changes according to your DB setup. To execute the service run uvicorn like so:

```bash
$ uvicorn ompid:app
[...]
INFO:     Started server process [18986]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

API descriptions are accessible at http://127.0.0.1:8000/docs .
