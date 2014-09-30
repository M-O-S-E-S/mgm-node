'''
Created on Jan 30, 2013

@author: mheilman
'''

import cherrypy, os

from Region import Region
import threading
import time, uuid, requests, logging, logging.handlers, json
from os import listdir
from os.path import isfile, join
from Monitor import Monitor as MonitorWrapper
from cherrypy.process.plugins import Monitor
import cherrypy

import xml.etree.ElementTree as ET

class Slave:
    
    def __init__(self, conf):
        self.availablePorts = []
        self.registeredRegions = {}
        
        statsInterval = conf['interval']
        self.nodePort = conf['port']
        self.key = uuid.uuid4()
        self.host = conf['host']
        self.frontendAddress = conf['webAddress']
        self.binDir = conf['binDir']
        self.regionDir = conf['regionDir']
        self.publicAddress = conf['regionAddress']
        for r,c in zip(conf['regionPorts'],conf['consolePorts']):
            self.availablePorts.append({"port":r,"console":c})
        
        #we can't start up without our master config
        configLoaded = False
        while not configLoaded:
            try:
                configLoaded = self.loadRemoteConfig()
            except Exception, e:
                print "Error contacting MGM: %s" % e
                time.sleep(30)
    
        self.monitor = MonitorWrapper()
        
        Monitor(cherrypy.engine, self.updateStats, frequency=statsInterval).subscribe()
        cherrypy.engine.subscribe('stop',self.engineExit)
    
        print "node started"
    
    def engineExit(self):
        for name in self.registeredRegions:
            self.registeredRegions[name]["proc"].terminate()
    
    def __del__(self):
        self.procs = None
        self.monTimer.cancel()
        print "node stopping"
    
    def loadRemoteConfig(self):
        #load additional config from master service
        url = "http://%s/server/dispatch/node" % (self.frontendAddress)
        r = requests.post(url, data={'host':self.host, 'port':self.nodePort, 'key':self.key, 'slots': len(self.availablePorts)}, verify=False)
        if not r.status_code == requests.codes.ok:
            raise Exception("Error contacting MGM at %s" % url)
        
        result = json.loads(r.content)
        
        if not result["Success"]:
            raise Exception("Error loading config: %s" % result["Message"])
            
        if len(result['Regions']) > len(self.availablePorts):
            raise Exception("Error: too many regions for configured ports")
                
        for region in result['Regions']:
            port = self.availablePorts.pop(0)
            self.registeredRegions[region['name']] = {
                "proc": Region(port["port"],port["console"], region['name'], self.binDir, self.regionDir, self.frontendAddress, self.publicAddress),
                "port": port
            }
        return True
        
    def updateStats(self):
        self.monitor.updateStatistics()
        stats = {}
        stats['host'] = self.monitor.stats
        stats['processes'] = []
        
        for name,region in self.registeredRegions.iteritems():
            region["proc"].updateProcStats()
            p = {}
            p['name'] = name
            p['running'] = str(region["proc"].isRunning())
            p['stats'] = region["proc"].stats
            stats['processes'].append(p)
        
        url = "http://%s/server/dispatch/stats/%s" % (self.frontendAddress, self.host)
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
    def region(self, name, action):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"

        #perform the action
        if action == "add":
            #check if region already present here
            if name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region already exists on this Node"})
            try:
                port = self.availablePorts.pop(0)
            except Exception, e:
                return json.dumps({ "Success": False, "Message": "No slots remaining"})
            self.registeredRegions[name] = {
                "proc": Region(port["port"],port["console"], name, self.binDir, self.regionDir, self.frontendAddress, self.publicAddress),
                "port": port
            }
            return json.dumps({ "Success": True})
        elif action == "remove":
            #find region, and remove if found
            if not name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region not present"})
            if self.registeredRegions[name]["proc"].isRunning():
                return json.dumps({ "Success": False, "Message": "Region is still running"})
            port = self.registeredRegions[name]["port"]
            del self.registeredRegions[name]
            self.availablePorts.insert(0,port)
            return json.dumps({ "Success": True})
        elif action == "start":
            if not name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region not present"})
            self.registeredRegions[name]["proc"].start()    
            return json.dumps({ "Success": True})
        elif action == "stop":
            if not name in self.registeredRegions:
                return json.dumps({ "Success": False, "Message": "Region not present"})
            self.registeredRegions[name]["proc"].stop()
            return json.dumps({ "Success": True})
        else:
            return json.dumps({ "Success": False, "Message": "Unsupported Action"})
    
    @cherrypy.expose
    def loadIar(self, name, avatarName, avatarPassword, inventoryPath, job):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage iars"})
                         
        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)

        if self.registeredRegions[name]["proc"].loadIar(ready, report, inventoryPath, avatarName, avatarPassword):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
        
    @cherrypy.expose
    def saveIar(self, name, avatarName, avatarPassword, inventoryPath, job):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage iars"})
                         
        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)
        if self.registeredRegions[name]["proc"].saveIar(report, upload, inventoryPath, avatarName, avatarPassword):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
    
    @cherrypy.expose
    def saveOar(self, name, job):
        #veryify request is coming from the web frontend
        ip = cherrypy.request.headers["Remote-Addr"]
        if not ip == self.frontendAddress:
            print "INFO: Attempted region control from ip %s instead of web frontent" % ip
            return "Denied, this functionality if restricted to the mgm web app"
            
        if not name in self.registeredRegions:
            return json.dumps({ "Success": False, "Message": "Region not present"})
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})
            
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        upload = "http://%s/server/task/upload/%s" % (self.frontendAddress, job)
        if self.registeredRegions[name]["proc"].saveOar(report, upload):
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
        if not self.registeredRegions[name]["proc"].isRunning():
            return json.dumps({ "Success": False, "Message": "Region must be running to manage oars"})
            
        ready = "http://%s/server/task/ready/%s" % (self.frontendAddress, job)
        report = "http://%s/server/task/report/%s" % (self.frontendAddress, job)
        if self.registeredRegions[name]["proc"].loadOar(ready, report, merge, x, y, z):
            return json.dumps({ "Success": True})
        return json.dumps({ "Success": False, "Message": "An error occurred communicating with the region"})
