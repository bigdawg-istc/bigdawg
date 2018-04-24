# Docker Image for Vertica

Warning: This is very experimental. The level of testing has consisted of "Start server, run some sql, shutdown server, restart server, run some sql". What it does manage to do is get a vertica process up and running in a docker container. For the purposes of testing applications with fig and such, this might be sufficient, but don't let it anywhere near production.

The image creates a database called docker, with a blank dbadmin password.

## Usage:

Download the Vertica RPM from https://my.vertica.com and put it in this folder.
Then run:
```bash
docker build -t bluelabs/vertica .
```

### To run without a persistent datastore
```bash
docker run -P  bluelabs/vertica
```

### To run with a persistent datastore
```bash
docker run -P -v /path/to/vertica_data:/home/dbadmin/docker bluelabs/vertica
```

## Common problems:

##### ```/docker-entrypoint.sh: line 15: ulimit: open files: cannot modify limit: Operation not permitted```

Databases open and keep open a number of files simultaneously, and often require higher configured limits than most Linux distributions come with by default.

To resolve this, increase your maximum file handle ulimit size (`ulimit -n`).  You might find this in `/etc/security/limits.conf`.  Depending on your distribution, you may also need to change your Docker configuration for limits it sets on top of this, which you might find in `/etc/sysconfig/docker`.
