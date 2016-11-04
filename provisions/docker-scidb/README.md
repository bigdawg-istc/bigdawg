Docker SciDB
============

Scripts for building a <a href="http://www.docker.com/">Docker</a> image of the array database <a href="http://www.scidb.org/">SciDB 14.12</a>. NOTE: This is not the latest version of SciDB but the last one to offer a binary install. 

<h3>Files:</h3>
<ul>
	<li><code>LICENSE</code> - License file.</li>
	<li><code>README.md</code> - This file.</li>
	<li><code>conf</code> - SHIM configuration file.</li>
	<li>Docker files:
		<ul>
			<li><code>Dockerfile</code> - Docker file for building a Docker Image.</li>		
			<li><code>setup.sh</code> - Host script for removing old containers and images from host machine. Then, it creates a Docker image called "scidb_img".</li>
		</ul>
	</li>
	<li>Container files:
		<ul>
			<li><code>containerSetup.sh</code> - Script for setting up SciDB on a container. It also creates some test data.</li>		
			<li><code>iquery.conf</code> - IQUERY configuration file.</li>
			<li><code>startScidb.sh</code> - Container script for starting SciDB.</li>
			<li><code>stopScidb.sh</code> - Container script for stopping SciDB.</li>
			<li><code>scidb_docker.ini</code> - SciDB's configuration file.</li>
		</ul>
	</li>
</ul>


<h3>Prerequisites:</h3>
<ul>
	<li>Internet access.</li>
	<li><a href="http://www.docker.com/">Docker</a>.</li>
</ul>


<h3>Instructions:</h3>
<ol>
	<li>Clone the project and then go to the docker_scidb folder: <code>git clone https://github.com/albhasan/docker_scidb.git</code></li>
	<li>Modify the configuration file <em>scidb_docker.ini</em> according to your needs and your hardware. Here you can find a helper <a href="https://github.com/Paradigm4/configurator">application</a>.</li>
	<li>Enable <code>setup.sh</code> for execution <code>chmod +x setup.sh</code> and run it <code>./setup.sh</code>. This creates a new Docker image from the Dockerfile and then it starts a container. You can also create containers manually using a command like this: <code>docker run -d --name="scidb1" -p 49901:22 -p 49902:8083 --expose=5432 --expose=1239 scidb_img</code></li>
	<li>Log into the container: <code>ssh -p 49901 root@localhost</code>. The default password is <em>xxxx.xxxx.xxxx</em></li>
	<li>Execute the script <code>/home/root/./containerSetup.sh</code>. This script set SciDB up and then it runs a small query.</li>
</ol>


<h5>NOTES:</h5>
<ul>
	<li><code>containerSetup.sh</code> includes commands for moving postgres' files to a different folder. Mounting a docker volume on that folder enables storage of catalog data in the host.</li>
	<li>When using volumes, match user's ID of a container-user "scidb" to a host-user with the proper writing rights.</li>
</ul>


Compile SciDB in a container
============================

<ul>
	<li>Clone the project and CD to the docker_scidb folder: <code>git clone https://github.com/albhasan/docker_scidb.git</code></li>
	<li>Go to the <code>dev</code> folder</li>
	<li>Enable <code>setup.sh</code> for execution (<code>chmod +x setup.sh</code>) and run it (<code>./setup.sh</code>): This creates a new image from the Dockerfile in the <code>dev</code> folder. </li>
	<li>Start a container <code>docker run -d --name="scidb_dev1" -p 49901:22  --expose=22 --expose=1239 --expose=5432 scidb_dev_img</code></li>
	<li>Log in the container <code>ssh -p 49901 root@localhost</code>. The password is <em>xxxx.xxxx.xxxx</em> for all the users.</li>
	<li>Execute the script <code>/./containerSetup.sh</code>.</li>
</ul>



<h3>Compile r_exec:</h3>

Once finished compiling SciDB, it is possible to compile r_exec:

<ul>

<li>Install and run Rserve (as root):
	<ul>
	<li><code>yes | apt-get install r-base</code></li>
	<li><code>R</code></li>
	<li><code>install.packages('Rserve')</code></li>
	<li><code>40</code></li>
	<li><code>quit()</code></li>
	<li><code>no</code></li>
        <li><code>R CMD Rserve</code></li>
	</ul>
</li>
<li>Copy required files:
	<ul>
	<li><code>cp -r /home/scidb/dev_dir/scidbtrunk/stage/install/include/* /usr/include/</code></li>
	<li><code>mkdir /usr/include/boost</code></li>
	<li><code>cp -r /opt/scidb/14.12/3rdparty/boost/include/boost/* /usr/include/boost</code></li>
	</ul>
</li>
<li>Download and compile r_exec:
	<ul>
	<li><code>cd ~</code></li>
	<li><code>git clone https://github.com/Paradigm4/r_exec.git</code></li>
	<li><code>cd ~/r_exec</code></li>

	<li><code>export SCIDB=/home/scidb/dev_dir/scidbtrunk/stage/install</code></li>
	<li><code>make SCIDB=/home/scidb/dev_dir/scidbtrunk/stage/install</code></li>
	<li><code>cp *.so /home/scidb/dev_dir/scidbtrunk/stage/install/lib/scidb/plugins</code></li>
	</ul>
</li>
<li>Restart SciDB (as SciDB user):
	<ul>
	<li><code>su scidb</code></li>
	<li><code>cd ~</code></li>
	<li><code>export LC_ALL="en_US.UTF-8"</code></li>
	<li><code>export SCIDB_VER=14.8</code></li>
	<li><code>export SCIDB_INSTALL_PATH=/home/scidb/dev_dir/scidbtrunk/stage/install</code></li>
	<li><code>export PATH=$SCIDB_INSTALL_PATH/bin:$PATH</code></li>
	<li><code>/home/scidb/dev_dir/scidbtrunk/./run.py stop</code></li>
	<li><code>/home/scidb/dev_dir/scidbtrunk/./run.py start</code></li>
	</ul>
</li>
<li>Load the plugin and run a test:
	<ul>
	<li><code>iquery</code></li>
	<li><code>set lang afl;</code></li>
	<li><code>load_library('r_exec');</code></li>
        <li><code>r_exec(build(&lt;z:double&gt;[i=1:100,10,0],0),'expr=x&lt;-runif(1000);y&lt;-runif(1000);list(sum(x^2+y^2&lt;1)/250)');</code></li>
	</ul>
</li>
</ul>


<h3>Compile geosdb</h3>
A wrapper for using Boost::geometry from inside SciDB. Follow this <a href="http://github.com/albhasan/geosdb">link</a>.
