# README #

Docker images for BigDAWG engines.

## How To Start

* Make sure that you have packaged the bigdawg maven project and have `istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar` under `target`
	* Example: `mvn clean package -P mit -DskipTests`

* If you're using macOS..

	* Install [Docker Toolbox](https://www.docker.com/products/docker-toolbox) from the docker official website
	* Run the `Docker Quickstart Terminal` application to open a special terminal
	* Change directory to `provisions`
	* Run `build_all_images.sh` to build all images
	* Use command `docker ps -a` to check if you have previously ran some of the containers
	* Use command `docker rm [CONTAINER IDs]` to kill off the old containers
	* Run `run_all_containers.sh` to boot the built images -- the process may take a while, especially when initializing the Accumulo containers
	* Run `cleanup_containers.sh` to stop and remove any containers before running them again

* If you're using Linux..
	* `build_all_images.sh`: Execute this first. Builds all images.
	* `run_all_containers.sh`: Run this second. Runs the built images after executing the above.
	* Run `cleanup_containers.sh` to stop and remove any containers before running them again
	
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
