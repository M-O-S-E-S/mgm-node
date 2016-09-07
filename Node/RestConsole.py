
import requests, xml.etree.ElementTree as ET
            
class RestConsole():
    
    def __init__(self, url, username, password):
        self.url = url.strip()
        if self.url[-1] != '/':
            self.url += "/"

        url = self.url + "StartSession/"
        
        r = requests.post(url, data={ 'USER': username, 'PASS': password}, verify=False)
        xml = ET.fromstring(r.content)
        self.session = xml.find("SessionID").text
        
    def close(self):
        url = self.url + "CloseSession/"
        r = requests.post(url, data={ 'ID': self.session}, verify=False)
        
    def write(self, cmd):
        url = self.url + "SessionCommand/"
        requests.post(url, data={ 'ID': self.session, 'COMMAND': cmd}, verify=False)
        
    def read(self):
        lines = []
        url = self.url + "ReadResponses/%s/" % self.session
        r = requests.post(url, data={ 'ID': self.session}, verify=False)
        return r.content
        
    def readLine(self):
        url = self.url + "ReadResponses/%s/" % self.session
        r = requests.post(url, data={ 'ID': self.session}, verify=False)
        xml = ET.fromstring(r.content)
        for node in xml.findall("Line"):
            yield node.text
