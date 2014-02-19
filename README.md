mgmNode
=======

Command and Control script used to allow mgm to start and stop processes on a separate host.  MgmNode is designed to work with and assist mgm by starting and stopping Opensimulator processes, as well as handle long-running tasks, such as oar and iar functionality.  MgmNode also captures Opensimulator logs and performance data, and pushes it into mgm.

Installation
---
MgmNode is a simple python application.  It requires an installed mgm instance be available on its network, with the ip address of the machine running mgmNode to be listed as a host by mgm.  MgmNode includes an example upstart script, as well as a py2exe script called freeze.py if running on Windows without an installed python runtime is necessary.

required packages
`python-cherrypy python-psutil python-requests`

Clone mgmNode into /opt

Copy /opt/mgmNode/mgm.cfg.example to /opt/mgmNode/mgm.cfg

Update mgm.cfg to match your distribution.

Test mgmNode by running node.py.  If all is configured correctly, including adding the ip address as a host to mgm, you will see a message repeating every 10 seconds with how many running and stopped processes mgm has recieved updates for.

You can run mgmNode manually, or in a screen session, but we recommend upstart or similar to handle this non-interactive service.