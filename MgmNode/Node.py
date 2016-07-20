'''
Created on Jan 30, 2013

@author: mheilman
'''

import cherrypy

from Region import Region
import time, uuid, requests, json, threading, os, shutil
from HostMonitor import HostMonitor
from cherrypy.process.plugins import Monitor
import cherrypy
from cherrypy.lib.static import serve_file

class Node:
    ''' A management class interfacing with region worker objects '''

    def __init__(self, conf):
        self.availablePorts = []
        self.registeredRegions = {}

        statsInterval = conf['interval']
        self.nodePort = conf['port']
        self.key = uuid.uuid4()
        self.host = conf['host']
        self.frontendURI = conf['webAddress'] + ":" + conf['webPort']
        self.frontendAddress = conf['webAddress']
        self.binDir = conf['binDir']
        self.regionDir = conf['regionDir']
        self.stashDir = conf['stashDir']
        self.publicAddress = conf['regionAddress']
        for r,c in zip(conf['regionPorts'],conf['consolePorts']):
            self.availablePorts.append(r)

        self.consoleUser = conf['consoleUser']
        self.consolePass = conf['consolePass']

        #we can't start up without our master config
        regions = False
        while not regions:
            try:
                regions = self.loadRemoteConfig()
            except Exception, e:
                print "Error contacting MGM: %s" % e
                time.sleep(30)

        self.initializeRegions(regions)

        self.monitor = HostMonitor()

        Monitor(cherrypy.engine, self.updateStats, frequency=statsInterval).subscribe()
        cherrypy.engine.subscribe('stop',self.engineExit)

        print "node started"

    def engineExit(self):
        #for name in self.registeredRegions:
        #    del self.registeredRegions[name]
        self.registeredRegions.clear()

    def loadRemoteConfig(self):
        #load additional config from master service
        url = "http://%s/server/dispatch/node" % (self.frontendURI)
        r = requests.post(url, data={'host':self.host, 'port':self.nodePort, 'key':self.key, 'slots': len(self.availablePorts)}, verify=False)
        if not r.status_code == requests.codes.ok:
            raise Exception("Error contacting MGM at %s" % url)

        result = json.loads(r.content)

        if not result["Success"]:
            raise Exception("Error loading config: %s" % result["Message"])

        if len(result['Regions']) > len(self.availablePorts):
            raise Exception("Error: too many regions for configured ports")
        return result['Regions']

    def initializeRegions(self, regions):
        '''create a Regions process for each assigned region'''
        #convert regions to a map
        regs = {}
        for r in regions:
            regs[r['uuid']] = r
        # Reuse assignments already placed on disk
        assignments = {}
        for id in os.listdir(self.regionDir):
            if id not in regs:
                # we have a region on disk that is not supposed to be ours, kill it and ignore its mapping
                r = Region(0, id, '', self.binDir, self.regionDir, self.frontendURI, self.publicAddress, self.consoleUser, self.consolePass)
                if r.isRunning:
                    r.kill()
                continue
            for line in open(os.path.join(self.regionDir, id, 'Halcyon.ini'), 'r'):
                if "http_listener_port" in line:
                    assignments[id] = int(line.split('"')[1])

        for id,port in assignments.iteritems():
            self.availablePorts.remove(port)
            region = regs[id]
            del regs[id]
            self.registeredRegions[id] = Region(
                port,
                region['uuid'],
                region['name'],
                self.binDir,
                self.regionDir,
                self.frontendURI,
                self.publicAddress,
                self.consoleUser,
                self.consolePass)
        # perform initial assignment
        for region in regs:
            port = self.availablePorts.pop(0)
            self.registeredRegions[region['uuid']] = Region(
                port["port"],
                region['uuid'],
                region['name'],
                self.binDir,
                self.regionDir,
                self.frontendURI,
                self.publicAddress,
                self.consoleUser,
                self.consolePass)

        # any regions that have recovered set to True will have self-assigned an http port



    def updateStats(self):
        self.monitor.updateStatistics()
        stats = {}
        stats['host'] = self.monitor.stats
        stats['processes'] = []

        for id,region in self.registeredRegions.iteritems():
            p = {}
            p['id'] = id
            p['running'] = str(region.isRunning)
            p['stats'] = region.stats
            stats['processes'].append(p)

        url = "http://%s/server/dispatch/stats/%s" % (self.frontendURI, self.host)
        try:
            r = requests.post(url, data={"json": json.dumps(stats)}, verify=False)
        except requests.ConnectionError:
            print "error connecting to master"
            return
        if not r.status_code == requests.codes.ok:
            print "error uploading stats to master"
        else:
            print "%s - Upload Status: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), r.content)

    @cherrypy.expose
    def index(self):
        return "<html><body><h1>MOSES Grid Manager Node: %s</h1></body></html>" % self.host

    # FRONT END FUNCTION CALLS

    @cherrypy.expose
    def add(self, id, name):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality if restricted to the mgm web app"})

        #check if region already present here
        if id in self.registeredRegions:
            print "Add region %s failed: Region already present on node" % id
            return json.dumps({ "Success": False, "Message": "Region already exists on this Node"})
        try:
            port = self.availablePorts.pop(0)
        except Exception, e:
            print "Add region %s failed: No Available Ports" % id
            return json.dumps({ "Success": False, "Message": "No slots remaining"})
        self.registeredRegions[id] = Region(
            port["port"],
            id,
            name,
            self.binDir,
            self.regionDir,
            self.frontendURI,
            self.publicAddress,
            self.consoleUser,
            self.consolePass)
        print "Add region %s succeeded" % id
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def remove(self, id):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality if restricted to the mgm web app"})

        #find region, and remove if found
        if not id in self.registeredRegions:
            print "Remove region %s failed: Region not present on node" % id
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if self.registeredRegions[id].isRunning:
            print "Remove region %s failed: Region is currently running" % id
            return json.dumps({ "Success": False, "Message": "Region is still running"})
        port = self.registeredRegions[id]["port"]
        del self.registeredRegions[id]
        self.availablePorts.insert(0,port)
        print "Remove region %s succeeded" % id
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def start(self, id):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality if restricted to the mgm web app"})
        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        self.registeredRegions[id].start()
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def stop(self, id):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality if restricted to the mgm web app"})
        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        self.registeredRegions[id].stop()
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def kill(self, id):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality if restricted to the mgm web app"})
        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        self.registeredRegions[id].kill()
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def saveOar(self, id, job):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"

        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[id].isRunning:
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})

        report = "http://%s/task/report/%s" % (self.frontendURI, job)
        upload = "http://%s/task/upload/%s" % (self.frontendURI, job)
        if self.registeredRegions[id].saveOar(report, upload):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})

    @cherrypy.expose
    def loadOar(self, name, job, merge, x, y, z):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"

        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name].isRunning:
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})

        ready = "http://%s/server/task/ready/%s" % (self.frontendURI, job)
        report = "http://%s/server/task/report/%s" % (self.frontendURI, job)
        if self.registeredRegions[name].loadOar(ready, report, merge, x, y, z):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
