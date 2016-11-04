FROM bigdawg/postgres-base

# Postgres listens on this port
# See https://www.postgresql.org/docs/9.4/static/app-postgres.html
ENV PGPORT 5430

ADD setup_postgres2.sh /docker-entrypoint-initdb.d/
