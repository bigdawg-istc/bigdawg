import docker_client


"""
Example 1) Run a container:
>>>docker_client.run("bigdawg-postgres-catalog")
'aa49e8d3b41b6a1fe1aea7bc66dbbb7fec879e0230a653499b5b31cb2f692017'

Example 2) Get a running container's info
>>> docker_client.ps("bigdawg-postgres-catalog")
'26eb2cec8399        bigdawg/postgres-catalog   "/bin/sh -c /image_co"   1 seconds ago       Up Less than a second   0.0.0.0:5400->5400/tcp, 0.0.0.0:8080->8080/tcp   bigdawg-postgres-catalog'

Example 3) Verify container is running
>>> docker_client.is_container_running("bigdawg-postgres-catalog")
True

Example 4) Stop a container:
'bigdawg-postgres-catalog'

"""

print "Attempting to run container bigdawg-postgres-catalog"
out = docker_client.run("bigdawg-postgres-catalog")
print out+"\n"

print "Attempting to get container info"
out = docker_client.ps("bigdawg-postgres-catalog")
print out+"\n"

print "Attempting to check if container is running"
out = docker_client.is_container_running("bigdawg-postgres-catalog")
print str(out)+"\n"

print "Attempting to `rm -f` container"
out = docker_client.rm("bigdawg-postgres-catalog")
print out+"\n"
