"""
Examples for using the DockerCient code, along with their printed output.
"""

import DockerClient

print ("Attempting to run container bigdawg-postgres-catalog")
out = DockerClient.run("bigdawg-postgres-catalog")
print (out+"\n")
# >'aa49e8d3b41b6a1fe1aea7bc66dbbb7fec879e0230a653499b5b31cb2f692017'

print ("Attempting to get container info")
out = DockerClient.ps("bigdawg-postgres-catalog")
print (out+"\n")
# >'26eb2cec8399        bigdawg/postgres-catalog   "/bin/sh -c /image_co"   1 seconds ago       Up Less than a second   0.0.0.0:5400->5400/tcp, 0.0.0.0:8080->8080/tcp   bigdawg-postgres-catalog'

print ("Attempting to check if container is running")
out = DockerClient.is_container_running("bigdawg-postgres-catalog")
print (str(out)+"\n")
# >DockerClient.is_container_running("bigdawg-postgres-catalog")

print ("Attempting to `rm -f` container")
out = DockerClient.rm("bigdawg-postgres-catalog")
print (out+"\n")
# > 'bigdawg-postgres-catalog'
