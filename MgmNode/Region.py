'''
Created on Feb 1, 2013

@author: mheilman
'''
import os, psutil, json, time, requests, threading, Queue as QX, re, shutil, time
from multiprocessing import Process, Queue
from threading import Thread
from subprocess import PIPE

from psutil import Popen

from RestConsole import RestConsole

from twisted.internet import reactor, protocol, task

PSUTIL2 = psutil.version_info >= (2, 0)

class RegionLogger( protocol.ProcessProtocol ):
    """A twisted process protocol to capture logging data from the opensim process"""

    def __init__(self, url, region, receiveLog):
        self. messages = []
        self.regexp = re.compile(r'^{[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}}.*')
        self.url = "http://%s/server/dispatch/logs/%s" % (url,region)
        self.region = region
        self.isRunning = False
        self.sendLog = receiveLog
        
        recurring = task.LoopingCall(self.reportLogs)
        recurring.start(10)

    def connectionMade(self):
        print "Region %s started" % self.region
        self.isRunning = True
        
    def outReceived(self, line):
        self.sendLog(line)
        if self.regexp.search(line) is not None:
            parts = line.split("-", 1)
            log = {}
            log["timestamp"] = "%s %s" % (time.strftime("%Y-%m-%d"), parts[0].strip())
            log["message"] = parts[1].strip()
            self.messages.append(log)
        else:
            log = {}
            log["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            log["message"] = line
            self.messages.append(log)
    
    def errReceived(self, line):
        if self.regexp.search(line) is not None:
            parts = line.split("-", 1)
            log = {}
            log["timestamp"] = "%s %s" % (time.strftime("%Y-%m-%d"), parts[0].strip())
            log["message"] = parts[1].strip()
            self.messages.append(log)
        else:
            log = {}
            log["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            log["message"] = line
            self.messages.append(log)
    
    def inConnectionLost(self):
        pass
        
    def outConnectionLost(self):
        pass
        
    def errConnectionLost(self):
        pass
        
    def processExited(self, reason):
        self.isRunning = False
        
    def processEnded(self, reason):
        self.isRunning = False
        
    def reportLogs(self):
        if len(self.messages) > 0:
            req = requests.post(self.url,data={'log':json.dumps(self.messages)}, verify=False)
            self.messages = []
            if not req.status_code == requests.codes.ok:
                print "Error sending %s: %s" % (self.region, req.content)
	

class RegionWorker:
    """a companion class to RegionLogger to monitor the application output for specific tasks"""
    
    def __init__(self, jobQueue):
        self.queue = jobQueue
        self.shutdown = False
        self.currentJob = False
        self.processLog = False
        reactor.callLater(10, self.pollForWork)
        
    def pollForWork(self):
        try:
            job = self.queue.get(False)
        except QX.Empty:
            reactor.callLater(10, self.pollForWork)
            return
        #change job state and trigger job
        self.currentJob = job
        print "Starting job %s on region [unspecified]" % (job['name'])
        if job['name'] == "save_oar":
            self.startSaveOar()
        elif job['name'] == "load_oar":
            self.startLoadOar()
        elif job['name'] == "save_iar":
            self.startSaveIar()
        elif job['name'] == "load_iar":
            self.startLoadIar()
        else:
            print "Error, invalid job type: %s" % job['name']
        #do not repoll until job is complete
        
    def receiveLog(self, line):
        if(self.processLog):
            self.processLog(line)
    
    def startLoadIar(self):
        print "[MGM] starting load iar task"
        self.startTime = time.time()
        cmd = "load iar %s %s %s %s" % (self.currentJob['user'], self.currentJob['path'], self.currentJob['password'], self.currentJob['ready'])
        self.currentJob['console'].write(cmd);
        self.currentJob['console'].close()
        self.processLog = self.processLoadIar
        
    def processLoadIar(self, line):
        if (time.time() - self.startTime) > 60*60:
            #timeout, report and schedule next job
            r = requests.post(self.currentJob['report'], data={"Success": False, "Done": True, "Message": "Timeout.  Iar load took too long"}, verify=False)
            print "load iar did not complete within time limit"
            self.processLog = False
            reactor.callLater(10, self.pollForWork)
        if not "[INVENTORY ARCHIVER]" in line:
            return
        if "Successfully" in line:
            #job done, report and schedule next job
            r = requests.post(self.currentJob['report'], data={"Success": True, "Done": True}, verify=False)
            print "load iar completed successfully"
            self.processLog = False
            reactor.callLater(10, self.pollForWork)
                
    def startSaveIar(self):
        print "[MGM] starting save iar task"
        self.startTime = time.time()
        iarName = "%s.iar" % (self.currentJob['user'].replace(" ",""))
        cmd = "save iar %s %s %s %s" % (self.currentJob['user'], self.currentJob['path'], self.currentJob['password'], iarName)
        self.currentJob['console'].write(cmd);
        self.currentJob['console'].close()
        self.processLog = self.processSaveIar
        
    def processSaveIar(self, line):
        if (time.time() - self.startTime) > 60*60:
            #timeout, report and schedule next job
            r = requests.post(self.currentJob['report'], data={"Success": False, "Done": True, "Message": "Timeout.  Iar save took too long"}, verify=False)
            self.log.put("save iar did not complete within time limit")
            self.processLog = False
            reactor.callLater(10, self.pollForWork)
        if not "[INVENTORY ARCHIVER]" in line:
            return
        if "Saved archive" in line:
            iarName = "%s.iar" % (self.currentJob['user'].replace(" ",""))
            iar = os.path.join(self.currentJob["location"],iarName)
            requests.post(self.currentJob['upload'], data={"Success": True}, files={'file': (self.currentJob['user'], open(iar, 'rb'))}, verify=False)
            os.remove(iar)
            print "load iar completed successfully"
            self.processLog = False
            reactor.callLater(10, self.pollForWork)

    def startLoadOar(self):
        print "[MGM] starting load oar task"
        self.startTime = time.time()
        if self.currentJob['merge'] == "1":
            cmd = "load oar --merge --force-terrain --force-parcels --displacement <%s,%s,%s> %s" % (self.currentJob['x'], self.currentJob['y'], self.currentJob['z'], self.currentJob['ready'])
        else:
            cmd = "load oar --displacement <%s,%s,%s> %s" % (self.currentJob['x'], self.currentJob['y'], self.currentJob['z'], self.currentJob['ready'])
        self.currentJob['console'].write(cmd);
        self.currentJob['console'].close()
        self.processLog = self.processLoadOar
        
    def processLoadOar(self,line):
        if (time.time() - self.startTime) > 60*60:
            #timeout, report and schedule next job
            r = requests.post(self.currentJob['report'], data={"Success": False, "Done": True, "Message": "Timeout.  Oar load took too long"}, verify=False)
            self.log.put("load oar did not complete within time limit")
            self.processLog = False
            reactor.callLater(10, self.pollForWork)
        if not "[ARCHIVER]" in line:
            return
        if "Successfully" in line:
            r = requests.post(self.currentJob['report'], data={"Success": False, "Done": True, "Message": "Unknown error"}, verify=False)
            print "load oar completed successfully"
            self.processLog = False
            reactor.callLater(10, self.pollForWork)
    
    def startSaveOar(self):
        print "[MGM] starting save oar task"
        self.startTime = time.time()
        cmd = "save oar mgm.oar"
        self.currentJob['console'].write(cmd);
        self.currentJob['console'].close()
        self.processLog = self.processSaveOar
        
    def processSaveOar(self, line):
        if (time.time() - self.startTime) > 60*60:
            #timeout, report and schedule next job
            r = requests.post(self.currentJob['report'], data={"Success": False, "Done": True, "Message": "Timeout.  Oar save took too long"}, verify=False)
            self.log.put("load oar did not complete within time limit")
            self.processLog = False
            reactor.callLater(10, self.pollForWork)
        if not "[ARCHIVER]" in line:
            return
        if "Finished" in line:
            oar = os.path.join(self.currentJob['location'],"mgm.oar")
            r = requests.post(self.currentJob['upload'], data={"Success": True}, files={'file': (self.currentJob['region'], open(oar, 'rb'))}, verify=False)
            os.remove(oar)
            print "save oar finished successfully"
            self.processLog = False
            reactor.callLater(10, self.pollForWork)

class Region:
    """A wrapper class aroudn a psutil popen instance of a region; handling logging and beckground tasks associated with this region"""
    def __init__(self, regionPort, consolePort, procName, binDir, regionDir, dispatchUrl, externalAddress):   
        self.stats = {}
        
        self.port = regionPort
        self.console = consolePort
        self.name = procName
        self.externalAddress = externalAddress
        self.dispatchUrl = dispatchUrl
        self.startString = "OpenSim.exe -console rest -name %%s -logconfig %s.cfg" % procName

        self.trackStage = "stopped"
        self.startFailCounter = 0
        self.stopFailCounter = 0
        self.simStatsCounter = 0
        self.simPhysicsCounter = 0
        
        self.jobQueue = Queue()
        self.worker = RegionWorker(self.jobQueue)
        self.pp = RegionLogger(self.dispatchUrl,self.name, self.worker.receiveLog)
        self.pid = 0
        
        if os.name != 'nt':
            self.startString = "mono %s" % self.startString
        else:
            self.startString = self.startDir + self.startString
            
        #clean up/create region folder
        self.startDir = os.path.join(regionDir, self.name)
        if not os.path.isdir(self.startDir):
            shutil.copytree(binDir, self.startDir)
        else:
			#update binaries if they have changed
			if os.path.getmtime(regionDir) > os.path.getmtime(self.startDir):
				shutil.rmtree(self.startDir)
				shutil.copytree(binDir, self.startDir)
        self.configFile = os.path.join(self.startDir, '%s.cfg' % procName)
        self.exe = os.path.join(self.startDir, 'OpenSim.exe')
            
    def isRunning(self):
        return self.pp.isRunning
    
    def getJsonStats(self):
        stats = self.stats
        return json.dumps(stats)
    
    def update(self):
        #update stats
        self.updateProcStats()
      
    def updateProcStats(self):
        """we can access most of this for the web, but we are compiling it to send xml to MGM"""
        stats = {}
        stats["stage"] = self.trackStage
        stats["timestamp"] = time.time()
        if self.isRunning():
            ptil = psutil.Process(self.pid.pid)
            ctime = ptil.create_time() if PSUTIL2 else ptil.create_time
            stats["uptime"] = time.time() - ctime
            stats["memPercent"] = ptil.get_memory_percent()
            stats["memKB"] = ptil.get_memory_info().rss / 1024
            stats["cpuPercent"] = ptil.get_cpu_percent(0.1)
            try:
                r = requests.get("http://127.0.0.1:%d/jsonSimStats" % self.port, timeout=0.5)
                if r.status_code == requests.codes.ok:
                    stats["simStats"] = json.loads(r.content)
            except:
                pass
        self.stats =  stats
           
    def writeConfig(self):
        # opensim.ini file
        r = requests.get("http://%s/server/dispatch/process/%s?httpPort=%s&consolePort=%s&externalAddress=%s" % (self.dispatchUrl, self.name, self.port, self.console, self.externalAddress))
        if r.status_code == requests.codes.ok:
            content = json.loads(r.content)
            region = content["Region"]
            f = open(os.path.join(self.startDir, 'OpenSim.ini'), 'w')
            for section in region:
                f.write('[%s]\n' % section)
                for item in region[section]:
                    f.write('\t%s = "%s"\n' %(item, region[section][item]))
                    if section == "Network":
                        if item == "ConsoleUser":
                            self.consoleUser = region[section][item]
                        if item == "ConsolePass":
                            self.consolePassword = region[section][item]
            f.close()
        
        # regions file
        r = requests.get("http://%s/server/dispatch/region/%s" % (self.dispatchUrl, self.name))
        if r.status_code == requests.codes.ok:
            content = json.loads(r.content)
            region = content["Region"]
            f = open(os.path.join(self.startDir, 'Regions', 'Regions.ini'), 'w')
            f.write("[%s]\n" % self.name)
            f.write('RegionUUID = "%s"\n' % region["uuid"])
            f.write('Location = "%s,%s"\n' % (region["locX"], region["locY"]))
            f.write('InternalAddress = "0.0.0.0"\n')
            f.write('InternalPort = %s\n' % region["httpPort"])
            f.write('SizeX=%d\n' % (256*int(region["size"])))
            f.write('SizeY=%d\n' % (256*int(region["size"])))
            f.write('AllowAlternatePorts = False\n')
            f.write('ExternalHostName = "%s"\n' % region["externalAddress"])
            f.write('SyncServerAddress = 127.0.0.1\n')
            f.write('SyncServerPort = 15000\n')
            f.close()
        
        # logging config file
        cfgFile = open(self.configFile, "w")
        cfgFile.write( """<?xml version="1.0" encoding="utf-8" ?>
            <configuration>
              <configSections>
                <section name="log4net" type="log4net.Config.Log4NetConfigurationSectionHandler,log4net" />
              </configSections>
              <runtime>
                <gcConcurrent enabled="true" />
                    <gcServer enabled="true" />
              </runtime>
              <appSettings></appSettings>
              <log4net>
                <appender name="Console" type="OpenSim.Framework.Console.OpenSimAppender, OpenSim.Framework.Console">
                  <layout type="log4net.Layout.PatternLayout">
                    <conversionPattern value="%date{{HH:mm:ss}} - %message" />
                  </layout>
                </appender>
                <appender name="StatsLogFileAppender" type="log4net.Appender.FileAppender">
                  <file value="OpenSimStats.log"/>
                  <appendToFile value="true" />
                  <layout type="log4net.Layout.PatternLayout">
                    <conversionPattern value="%date - %message%newline" />
                  </layout>
                </appender>
                <root>
                  <level value="DEBUG" />
                  <appender-ref ref="Console" />
                </root>
                <logger name="OpenSim.Region.ScriptEngine.XEngine">
                  <level value="INFO"/>
                </logger>
                <logger name="special.StatsLogger">
                  <appender-ref ref="StatsLogFileAppender"/>
                </logger>
              </log4net>
            </configuration>
            """)
        cfgFile.close()
    
    def start(self):
        if self.isRunning():
            return
        self.writeConfig()
        namedString = self.startString % self.name
        starter = namedString.split(" ")
        self.pid = reactor.spawnProcess(self.pp, starter[0], args=starter,path=self.startDir)
    
    def stop(self):
        self.pp.transport.signalProcess("KILL")
        
    def stopProcess(self):
        if self.isRunning():
            self.logQueue.put("[MGM] %s Terminating" % self.name)
            self.jobQueue.shutdown = True
            try:
                self.proc.kill()
            except psutil.NoSuchProcess:
                pass
            self.logQueue.shutdown = True

    def saveOar(self, reportUrl, uploadUrl):
        try:
            print "[MGM] %s requested save oar" % self.name
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, self.consoleUser, self.consolePassword)
            self.jobQueue.put({"name": "save_oar", "report": reportUrl, "upload": uploadUrl, "console": console, "region":self.name, "location":self.startDir})
        except:
            return False
        return True
        
    def loadOar(self, ready, report, merge, x, y, z):
        try:
            print "[MGM] %s requested load oar" % self.name
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, self.consoleUser, self.consolePassword)
            self.jobQueue.put({"name": "load_oar", "ready": ready, "report": report, "console": console, "merge": merge, "x":x, "y":y, "z":z})
        except:
            print "exception occurred"
            return False
        return True
        
    def loadIar(self, ready, reportUrl, invPath, avatar, password):
        try:
            print "[MGM] User requested load iar"
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, self.consoleUser, self.consolePassword)
            self.jobQueue.put({"name": "load_iar", "ready": ready, "report": reportUrl, "console": console, "path": invPath, "user":avatar, "password":password})
        except:
            return False
        return True
        
    def saveIar(self, reportUrl, uploadUrl, invPath, avatar, password):
        try:
            print "[MGM] User requested save iar"
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, self.consoleUser, self.consolePassword)
            self.jobQueue.put({"name": "save_iar", "report": reportUrl, "upload": uploadUrl, "console": console, "path": invPath, "user":avatar, "password":password, "location":self.startDir})
        except:
            return False
        return True
