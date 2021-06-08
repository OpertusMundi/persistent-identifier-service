FROM python:3.8-slim AS build-stage-1

RUN apt-get update && apt-get install -y gcc make libpq-dev
RUN pip3 install --no-cache-dir psycopg2==2.8.5 SQLAlchemy==1.3.17 


FROM python:3.8-slim

RUN apt-get update && apt-get install -y libpq5 \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=build-stage-1 /usr/local/ /usr/local/

RUN pip3 install --no-cache-dir requests==2.25.1 PyYAML==5.3.1 pydantic==1.5.1 fastapi==0.55.1 uvicorn==0.12.3 

RUN mkdir /usr/local/persistent-identifier-service 
WORKDIR /usr/local/persistent-identifier-service

COPY setup.py ./
COPY ompid ./ompid
RUN pip3 install --no-cache-dir --no-deps .

RUN groupadd -g 1000 uvicorn \
  && useradd -u 1000 -m -d /var/local/persistent-identifier-service -g uvicorn uvicorn

COPY docker-command.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/docker-command.sh

WORKDIR /var/local/persistent-identifier-service

COPY --chown=uvicorn:uvicorn settings.yml.template ./settings.yml

ENV \
  DATABASE_URL="postgresql://localhost:5432/pid" \
  DATABASE_USERNAME="pid" \
  DATABASE_PASSWORD_FILE="/secrets/database-password"

USER uvicorn
CMD ["/usr/local/bin/docker-command.sh"]
