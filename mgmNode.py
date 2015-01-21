#!/usr/bin/python
'''
Created on Sep 22, 2011

@author: mheilman
'''
import sys, os, ConfigParser

from MgmNode.Slave import Slave

import socket, string

from OpenSSL import crypto, SSL

from twisted.web import server, resource
from twisted.internet import reactor, ssl

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
    conf['certFile'] = config.get('ssl', 'cert')
    conf['keyFile'] = config.get('ssl', 'key')
    conf['interval'] = int(config.get('node', 'sample_interval'))
    
    portRange = config.get('node','region_port_range')
    consoleRange = config.get('node','console_port_range')
    
    vals = string.split(portRange,'-')
    conf['regionPorts'] = range(int(vals[0]),int(vals[1])+1)
        
    vals = string.split(consoleRange,'-')
    conf['consolePorts'] = range(int(vals[0]),int(vals[1])+1)

    return conf
            

os.chdir(modulePath())
conf = loadConfig(os.path.join(modulePath() ,'mgm.cfg'))
if not os.path.isfile(conf['certFile']) or not os.path.isfile(conf['keyFile']):
    generateCerts(conf['certFile'], conf['keyFile'])
site = server.Site(Slave(conf))
reactor.listenSSL(conf['port'], site, ssl.DefaultOpenSSLContextFactory(conf['keyFile'],conf['certFile']))
reactor.run()
