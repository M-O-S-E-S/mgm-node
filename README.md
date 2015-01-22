# mgmNode

Command and Control script used to allow mgm to start and stop processes on a separate host.  MgmNode is designed to work with and assist mgm by starting and stopping Opensimulator processes, as well as handle long-running tasks, such as oar and iar functionality.  MgmNode also captures Opensimulator logs and performance data, and pushes it into mgm.

## Ubuntu / Centos 6
Note that the version of python-requests on Ubuntu is outdated on Ubuntu 12.04, so you must either acquire the required package, or run on a newer version of Ubuntu.

On centos, all requisite packages at appropriate versions are present in EPEL

### Required Packages

python-psutil python-requests python-twisted

### Installation
MgmNode is a simple python application.  It requires an installed mgm instance be available on its network, with the ip address of the machine running mgmNode to be listed as a host by mgm.  MgmNode includes an example upstart script, as well as a py2exe script called freeze.py if running on Windows without an installed python runtime is necessary.

MgmNode is currently tested on python 2.7, and will run wherever that, and its required packages are available.  

1. Clone mgmNode into /opt
1. get Opensim 0.8.1 and unzip into /opt
1. Create directory /opt/regions
1. Copy /opt/mgmNode/mgm.cfg.example to /opt/mgmNode/mgm.cfg
1. Update mgm.cfg to match your file layout, and network ports.
1. Run `python mgmNode/mgmNode.py` to confirm all apackages present and test isntallation.  If you have many ports configured this may take a few minutes before it prints to the console, as it is performing a file copy from opensim into regions
1. Add the ip address of your mgmNode host to MGM.  If they are on the same host, use external ip addresses instead of 127.0.0.1
1. Reference the mgmNode/upstart script for running mgmNode as a service
1.  Allow ports 8080 and your configured ports through iptables if necessary

## Windows
mgmNode has been installed an ran on a Windows server, but Windows specific directions are not included here
