import docker
import socket
        

class DockerClient(object):

    version = 'auto'
    
    # saw has an old version of docker
    if socket.getfqdn() == 'saw.csail.mit.edu':
        version = '1.23'

    # Don't show DataHub containers
    container_blacklist = ['db', 'app', 'web', 'data', 'logs']
    
    # Only show containers starting with 'bigdawg'
    container_prefix = 'bigdawg'

    client = docker.from_env(version=version)


    def list_containers(self):
        containers = self.client.containers.list(all=True)
        containers = [{'name': c.name.lower(), 'status': c.status} for c in containers]
        containers = [c for c in containers if c["name"] not in self.container_blacklist]
        containers = [c for c in containers if c["name"].startswith(self.container_prefix)]
        return containers


    def start_container(self, name):
        if name.lower() not in self.container_blacklist and name.lower().startswith(self.container_prefix):
            container = self.client.containers.get(name)
            container.start()
        return container


    def stop_container(self, name):
        if name.lower() not in self.container_blacklist and name.lower().startswith(self.container_prefix):
            container = self.client.containers.get(name)
            container.stop()
        return container

# from subprocess import Popen, PIPE
# """
# This script contains several utilities for the BigDAWG admin iterface
# """

# """
# Returns container info for a running container given the container name
# input: ctr_name string, container name to get info for
# output: output from "docker ps -f name="container name"
#     * warning: filter on name accepts partial matches
# """
# def ps(ctr_name):
#     p = Popen('docker ps -f name="%s"' % (ctr_name), shell=True, stdout=PIPE, stderr=PIPE)
#     out = p.stdout.read()
#     if p.wait() != 0:
#         raise RuntimeError(p.stderr.read())
    
#     out = out.split('\n')[1]  # only return first line after header
#     return out.rstrip()


# """
# Check if a container is running. Does a weak check on `docker ps` output
# input: ctr_name string, container name
# output: True/False
# """
# def is_container_running(ctr_name):
#     p = Popen('docker ps -f name="%s" -f status=running --format "{{.Names}}"' % (ctr_name), shell=True, stdout=PIPE, stderr=PIPE)
#     out = p.stdout.read()
#     if p.wait() != 0:
#         raise RuntimeError(p.stderr.read())
#     if ctr_name in out.split('\n'):
#         return True
#     else:
#         return False


# """
# Runs a particular container given the container name
# input: ctr_name string, container to start
# output: none. Runs the container defined in the dict
# """
# def run(ctr_name):
#     # Build a dictionary of run commands for each container
#     run_cmd ={"bigdawg-postgres-catalog": "docker run -d --net=bigdawg -h bigdawg-postgres-catalog -p 5400:5400 -p 8080:8080 --name bigdawg-postgres-catalog bigdawg/postgres-catalog"}
#     # Execute the run command
#     if ctr_name in run_cmd:        
#         # quick check to see if already running
#         if is_container_running(ctr_name):
#             print ("Container %s is already running. Aborting." % ctr_name)
#             print ("Use rm(%s) to stop")
#             raise RuntimeError("Container is already running.")
#         else:
#             this_run_cmd = run_cmd[ctr_name]
#             p = Popen(this_run_cmd, shell=True, stdout=PIPE, stderr=PIPE)
#             out = p.stdout.read()
#             if p.wait() != 0:
#                 raise RuntimeError(p.stderr.read())
#             return out.rstrip()
#     else:
#         print ("invalid container name. Must be one of:", run_cmd.keys())


# """
# Removes container
# """
# def rm(ctr_name):
#     p = Popen("docker rm -f %s" % ctr_name, shell=True, stdout=PIPE, stderr=PIPE)
#     out = p.stdout.read()
#     if p.wait() != 0:
#         raise RuntimeError(p.stderr.read())
#     return out.rstrip()

