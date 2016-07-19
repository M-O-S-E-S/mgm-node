'''
Created on Feb 1, 2013

@author: mheilman
'''
import os, psutil, json, time, requests, threading, re, shutil, sys
from multiprocessing import Process, Queue
from Queue import Empty
from threading import Thread
from psutil import Popen

from RestConsole import RestConsole

# the pit in which to throw normal process output
DEVNULL = open(os.devnull, 'wb')

class Region:
    """A wrapper class aroudn a psutil popen instance of a region; handling logging and beckground tasks associated with this region"""

    proc = None
    stats = {}
    isRunning = False
    shuttingDown = False

    def __init__(self, regionPort, uuid, name, binDir, regionDir, dispatchUrl, externalAddress):
        self.port = regionPort
        self.id = uuid
        self.name = name
        self.externalAddress = externalAddress
        self.dispatchUrl = dispatchUrl
        self.startString = "Halcyon.exe -name %s"

        self.startDir = os.path.join(regionDir, self.id)
        if os.name != 'nt':
            self.startString = "mono %s" % self.startString
        else:
            self.startString = os.path.join(self.startDir, self.startString)

        self.logFile = os.path.join(self.startDir, 'Halcyon.log')

        #start job processing thread
        self.jobQueue = Queue()
        th = threading.Thread(target=self._doTasks)
        th.daemon = True
        th.start()
        self.jobQueue.put(("_checkBinaries", (binDir,)))

        #start process monitoring thread
        th = threading.Thread(target=self._monitorProcess)
        th.daemon = True
        th.start()

        #start log monitoring thread
        self.logQueue = Queue()
        th = threading.Thread(target=self._monitorLog)
        th.daemon = True
        th.start()

        #start log dispatch thread
        self.logConsumer = lambda line: None
        th = threading.Thread(target=self._dispatchLog)
        th.daemon = True
        th.start()

    def __del__(self):
        if self.proc:
            try:
                self.proc.kill()
            except:
                pass
        self.shuttingDown = True

    def _monitorProcess(self):
        """Monitor process and update statistics"""
        while not self.shuttingDown:
            if not self.proc:
                self.isRunning = False
            else:
                try:
                    if self.proc.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING, psutil.STATUS_DISK_SLEEP]:
                        self.isRunning = True
                    else:
                        self.isRunning = False
                except psutil.NoSuchProcess:
                    self.isRunning = False
            stats = {}
            stats["timestamp"] = time.time()
            if self.isRunning:
                try:
                    stats["uptime"] = time.time() - self.proc.create_time()
                    stats["memPercent"] = self.proc.memory_percent()
                    stats["memKB"] = self.proc.memory_info().rss / 1024
                    stats["cpuPercent"] = self.proc.cpu_percent(0.1)
                except:
                    stats = {}
            self.stats =  stats
            time.sleep(5)

    def _monitorLog(self):
        """Monitor log file and append new lines to the internal queue"""
        while not self.shuttingDown and not os.path.isfile(self.logFile):
            time.sleep(5)
        f = open(self.logFile, 'r')
        f.seek(0,2)
        while not self.shuttingDown:
            line = f.readline()
            if not line:
                time.sleep(5)
                continue
            self.logQueue.put(line)
        f.close()

    def _dispatchLog(self):
        """process log lines and upload to MGM for display"""
        lines = []
        while not self.shuttingDown:
            try:
                for i in range(self.logQueue.qsize()):
                    line = self.logQueue.get(False)
                    if line:
                        lines.append(line)
                        self.logConsumer(line)
            except Empty:
                pass

    def _doTasks(self):
        """Process asynchronous lambda tasks from internal queue"""
        import traceback
        while not self.shuttingDown:
            #block and wait for a new job
            try:
                (functor, args) = self.jobQueue.get(False)
            except Empty:
                time.sleep(5)
                continue
            except IOError:
                continue
            try:
                getattr(self, functor)(*args)
            except:
                print "Error processing job %s: %s" % (functor, sys.exc_info())
                print traceback.format_exc()
                # reset logConsumer in case it was modified
                self.logConsumer = lambda line: None

    def _checkBinaries(self, binDir):
        """make sure we are ready to start the process when necessary"""
        #clean up/create region folder
        if not os.path.isdir(self.startDir):
            shutil.copytree(binDir, self.startDir)
        else:
			#update binaries if they have changed
			if os.path.getmtime(binDir) > os.path.getmtime(self.startDir):
				shutil.rmtree(self.startDir)
				shutil.copytree(binDir, self.startDir)

    def start(self):
        """schedule the process to start"""
        self.jobQueue.put(("_start", ()))

    def _start(self):
        """start the process"""
        if self.isRunning:
            return

        # write the Halcyon.ini config file
        r = requests.get("http://%s/server/dispatch/process/%s?httpPort=%s&externalAddress=%s" % (self.dispatchUrl, self.id, self.port, self.externalAddress))
        if r.status_code != requests.codes.ok:
            print "Region %s failed to start, failed getting ini values from MGM"
            return
        content = json.loads(r.content)
        region = content["Region"]
        f = open(os.path.join(self.startDir, 'Halcyon.ini'), 'w')
        for section in region:
            f.write('[%s]\n' % section)
            for item in region[section]:
                f.write('\t%s = "%s"\n' %(item, region[section][item]))
        f.close()

        # write the Regions.cfg file
        r = requests.get("http://%s/server/dispatch/region/%s" % (self.dispatchUrl, self.id))
        if r.status_code != requests.codes.ok:
            print "Region %s failed to start, failed getting region values from MGM"
            return
        region = json.loads(r.content)["Region"]
        xml = """<Root><Config allow_alternate_ports="false" clamp_prim_size="false"
            external_host_name="{5}" internal_ip_address="0.0.0.0" internal_ip_port="{4}"
            lastmap_refresh="0" lastmap_uuid="00000000-0000-0000-0000-000000000000"
            master_avatar_first="first" master_avatar_last="last"
            master_avatar_pass="23459873204987wkjhbao873q4tr7u3q4of7"
            master_avatar_uuid="00000000-0000-0000-0000-000000000000"
            nonphysical_prim_max="0" object_capacity="0" outside_ip="{5}"
            physical_prim_max="0" region_access="0" region_product="0"
            sim_UUID="{0}" sim_location_x="{2}" sim_location_y="{3}"
            sim_name="{1}" /></Root>""".format(
                self.id,
                region["Name"],
                region["LocationX"],
                region["LocationY"],
                self.port,
                self.externalAddress
                )
        if not os.path.exists(os.path.join(self.startDir, 'Regions')):
            os.mkdir(os.path.join(self.startDir, 'Regions'))
        f = open(os.path.join(self.startDir, 'Regions', 'default.xml'), 'w')
        f.write(xml)
        f.close()

        namedString = self.startString % self.name
        self.proc = Popen(namedString.split(" "), cwd=self.startDir, stdout=DEVNULL, stderr=DEVNULL)

    def stop(self):
        """immediately terminate the process"""
        try:
            self.proc.kill()
        except psutil.NoSuchProcess:
            pass

    def saveOar(self, report, upload):
        """request that an oar file be saved from this process"""
        pass

    def _saveOar(self):
        """perform an oar save"""
        pass

    def loadOar(self, report, download):
        """request that an oar file be loaded into this process"""
        pass

    def _loadOar(self):
        """perform an oar load"""
        pass

"""
    def saveOar(self, reportUrl, uploadUrl):
        try:
            self.logQueue.put("[MGM] %s requested save oar" % self.name)
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, self.consoleUser, self.consolePassword)
            self.jobQueue.put({"name": "save_oar", "reportUrl": reportUrl, "uploadUrl": uploadUrl, "console": console, "region":self.name, "location":self.startDir})
        except:
            return False
        return True

    def loadOar(self, ready, report, merge, x, y, z):
        try:
            self.logQueue.put("[MGM] %s requested load oar" % self.name)
            url = "http://127.0.0.1:" + str(self.console)
            console = RestConsole(url, self.consoleUser, self.consolePassword)
            self.jobQueue.put({"name": "load_oar", "ready": ready, "report": report, "console": console, "merge": merge, "x":x, "y":y, "z":z})
        except:
            print "exception occurred"
            return False
        return True
"""
