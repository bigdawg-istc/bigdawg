FROM bigdawg/scidb

COPY image_contents/ /image_contents/
COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /image_contents/

CMD ["/bin/bash", "/image_contents/start_services.sh"]