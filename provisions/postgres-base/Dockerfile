# https://hub.docker.com/_/postgres/

FROM postgres:9.4.5

# set required environment variables for postgres
ENV POSTGRES_PASSWORD test

# add data file required by the next layer
ADD mimic2.pgd /tmp/mimic2/mimic2.pgd

