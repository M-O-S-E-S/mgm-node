#!/usr/bin/python
'''
Created on Sep 22, 2011

@author: mheilman
'''
import sys, os, cherrypy, ConfigParser

from MgmNode.Slave import Slave

import socket, string

from OpenSSL import crypto, SSL

def modulePath():
    if hasattr(sys,"frozen"):
        return os.path.dirname(unicode(sys.executable, sys.getfilesystemencoding( )))
    return os.path.dirname(unicode(__file__, sys.getfilesystemencoding( )))

def generateCerts(certFile, keyFile):
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 1024)

    cert = crypto.X509()
    cert.get_subject().C = "US"
    cert.get_subject().ST = "Florida"
    cert.get_subject().L = "Orlando"
    cert.get_subject().O = "US Army"
    cert.get_subject().OU = "ARL STTC"
    cert.get_subject().CN = socket.gethostname()
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10*365*24*60*60)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    cert.sign(k, 'sha1')

    open(certFile, "wt").write(
        crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    open(keyFile, "wt").write(
        crypto.dump_privatekey(crypto.FILETYPE_PEM, k))

def loadConfig(filePath):
    config = ConfigParser.ConfigParser()
    config.read(filePath)
    conf = {}
    conf['port'] = int(config.get('node','moses_slave_port'))
    conf['host'] = socket.gethostname();
    conf['regionAddress'] = config.get('node','region_external_address')
    conf['binDir'] = config.get('node','opensim_template')
    conf['regionDir'] = config.get('node','region_dir')
    conf['webAddress'] = config.get('node', 'mgm_address')
    conf['webPort'] = config.get('node', 'mgm_port')
    #conf['certFile'] = config.get('ssl', 'cert')
    #conf['keyFile'] = config.get('ssl', 'key')
    conf['interval'] = int(config.get('node', 'sample_interval'))

    portRange = config.get('node','region_port_range')
    consoleRange = config.get('node','console_port_range')

    vals = string.split(portRange,'-')
    conf['regionPorts'] = range(int(vals[0]),int(vals[1])+1)

    vals = string.split(consoleRange,'-')
    conf['consolePorts'] = range(int(vals[0]),int(vals[1])+1)

    return conf


if sys.platform == "win32":
    import win32serviceutil, win32service
    class NodeService(win32serviceutil.ServiceFramework):
        _svc_name_ = "MGMNode"
        _svc_display_name_ = "MGM Host Node"

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            cherrypy.engine.exit()
            self.ReportServiceStatus(win32service.SERVICE_STOPPED)

        def SvcDoRun(self):
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            localPath = modulePath()
            conf = loadConfig(os.path.join(localPath,'mgm.cfg'))
            app = Slave(conf)
            cherrypy.tree.mount(app, '/', config={'/': {}})
            cherrypy.config.update({
                'global':{
                    'server.socket_host':'0.0.0.0',
                    'server.socket_port':conf['port'],
                    'log.screen': True,
                    'log.error_file': os.path.join(modulePath(),"error.log"),
                    'log.access_file': os.path.join(modulePath(),"access.log"),
                    'engine.autoreload.on': False,
                    'engine.SIGHUP': None,
                    'engine.SIGTERM': None
                }
            })
            cherrypy.engine.start()
            cherrypy.engine.block()

def start():
    conf = loadConfig(os.path.join(modulePath() ,'mgm.cfg'))
    app = Slave(conf)
    #if not os.path.isfile(conf['certFile']) or not os.path.isfile(conf['keyFile']):
    #    generateCerts(conf['certFile'], conf['keyFile'])
    cherrypy.config.update({
        'global':{
            'server.socket_host':'0.0.0.0',
            'server.socket_port':conf['port'],
            'log.screen': True,
            'engine.autoreload.on': False,
            'engine.SIGHUP': None,
            'engine.SIGTERM': None,
            #'server.ssl_module': 'pyopenssl',
            #'server.ssl_certificate':conf['certFile'],
            #'server.ssl_private_key':conf['keyFile']
        }
    })
    cherrypy.quickstart(app, config={'/': {}})

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        start()
    else:
        if sys.platform == "win32":
            win32serviceutil.HandleCommandLine(NodeService)
        else:
            start()
