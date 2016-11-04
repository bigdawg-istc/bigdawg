FROM bigdawg/postgres-base

# Postgres listens on this port
# See https://www.postgresql.org/docs/9.4/static/app-postgres.html
ENV PGPORT 5431

ADD catalog /tmp/catalog
ADD monitor /tmp/monitor
ADD mimic2 /tmp/mimic2
ADD schemas /tmp/schemas
ADD tpch /tmp/tpch
ADD logs /tmp/logs
ADD setup_postgres1.sh /docker-entrypoint-initdb.d/
