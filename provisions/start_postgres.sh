#!/bin/bash
docker exec -d bigdawg-postgres-data1 java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-data1
docker exec -d bigdawg-postgres-data2 java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-data2
docker exec bigdawg-postgres-catalog java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-catalog
