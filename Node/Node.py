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
        self.registeredRegions = {}
        self.regionPorts = conf['regionPorts']
        statsInterval = conf['interval']
        self.nodePort = conf['port']
        self.host = conf['host']
        self.frontendURI = conf['webAddress'] + ":" + conf['webPort']
        self.frontendAddress = conf['webAddress']
        self.binDir = conf['binDir']
        self.regionDir = conf['regionDir']
        self.publicAddress = conf['regionAddress']

        #we can't start up without our master config
        success = False
        regions = []
        while not success:
            try:
                success, regions = self.loadRemoteConfig()
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
        print "loading config from MGM"
        #load additional config from master service
        url = "http://%s/node" % (self.frontendURI)
        r = requests.post(url, data={'name':self.host, 'port':self.nodePort, 'slots': self.regionPorts, 'public_ip': self.publicAddress}, verify=False)
        if not r.status_code == requests.codes.ok:
            raise Exception("Error contacting MGM at %s" % url)

        result = json.loads(r.content)

        if not result["Success"]:
            raise Exception("Error loading config: %s" % result["Message"])

        return True, result['Regions']

    def initializeRegions(self, regions):
        '''create a Regions process for each assigned region'''
        #convert regions to a map
        regs = {}
        for region in regions:
            self.registeredRegions[region['uuid']] = Region(
                region['uuid'],
                region['name'],
                self.binDir,
                self.regionDir,
                self.frontendURI,
                self.publicAddress)

    def updateStats(self):
        self.monitor.updateStatistics()
        stats = {}
        stats['host'] = self.monitor.stats
        stats['processes'] = []

        for id,region in self.registeredRegions.iteritems():
            p = {}
            p['id'] = id
            p['stats'] = region.stats
            stats['processes'].append(p)

        url = "http://%s/stats" % (self.frontendURI)
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
            return json.dumps({ "Success": False, "Message": "Denied, this functionality is restricted to the mgm web app"})

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
            port,
            id,
            name,
            self.binDir,
            self.regionDir,
            self.frontendURI,
            self.publicAddress)
        print "Add region %s succeeded" % id
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def remove(self, id):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality is restricted to the mgm web app"})

        #find region, and remove if found
        if not id in self.registeredRegions:
            print "Remove region %s failed: Region not present on node" % id
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if self.registeredRegions[id].isRunning:
            print "Remove region %s failed: Region is currently running" % id
            return json.dumps({ "Success": False, "Message": "Region is still running"})
        port = self.registeredRegions[id].port
        del self.registeredRegions[id]
        self.availablePorts.insert(0,port)
        print "Remove region %s succeeded" % id
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def start(self, id, ini, xml):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality is restricted to the mgm web app"})
        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        self.registeredRegions[id].start(ini, xml)
        return json.dumps({ "Success": True})

    @cherrypy.expose
    def kill(self, id):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return json.dumps({ "Success": False, "Message": "Denied, this functionality is restricted to the mgm web app"})
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
            return "Denied, this functionality is restricted to the mgm web app"

        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[id].isRunning:
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})

        report = "http://%s/report/%s" % (self.frontendURI, job)
        upload = "http://%s/upload/%s" % (self.frontendURI, job)
        if self.registeredRegions[id].saveOar(report, upload):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})

    @cherrypy.expose
    def loadOar(self, regionID, jobID, oarFile):
        print 'received load Oar call'
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality is restricted to the mgm web app"

        if not regionID in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[regionID].isRunning:
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})

        ready = "http://%s/ready/%s" % (self.frontendURI, jobID)
        report = "http://%s/report/%s" % (self.frontendURI, jobID)
        print 'oar file received'
	if self.registeredRegions[regionID].loadOar(ready, report, oarFile):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})

    @cherrypy.expose
    def consoleCmd(self, id, cmd):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality is restricted to the mgm web app"

        if not id in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[id].isRunning:
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})

        if self.registeredRegions[id].consoleCmd(cmd):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
