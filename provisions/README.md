# README #

Docker images for BigDAWG engines.

## How To Start

* `build_all_images.sh`: Execute this first. Builds all images.
* `run_all_containers.sh`: Run this second. Runs the built images after executing the above.

## Contents

**Baseline images**: This directory contains the following baseline images of clean database engines with no data:

* `postgres/`: Baseline postgres image.
* `scidb/`: Baseline scidb image.
* `docker-builds/accumulo/`: A fork of [https://github.com/sroegner/docker-builds](https://github.com/sroegner/docker-builds). Contains baseline accumulo image.

**Data images**: You may use the above images and populate them with your own test data. However, the following images add a layer of data to the baseline images:

* `postgres-catalog/`: Adds a BigDAWG catalog database data to the `postgres` image
* `postgres-data1/`: Adds Mimic II data to the `postgres` image
* `postgres-data2/`: Adds a copy of the Mimic II data to the `postgres` image. Will be used for demonstrating postgres-to-postgres migrations.
* `scidb-data/`: Adds Mimic II waveform data to the `scidb` image
* `accumulo-data/`: Adds Mimic II text data to the `docker-builds/accumulo` image

## Docker installation

You must have docker installed to use these images.

If on Mac or Windows, use [Docker Toolbox](https://docs.docker.com/toolbox/overview/). Do not use "Docker for Mac" or "Docker for Windows" because of known [networking limitations](https://docs.docker.com/docker-for-mac/networking/#/known-limitations-use-cases-and-workarounds). To launch a terminal, use the "Docker Quickstart Terminal" application, which opens a specially-configured terminal window for you.

If on Linux, then use the native linux docker.	
