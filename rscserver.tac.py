from twisted.application import internet, service
from twisted.application.internet import TimerService
from rscoin.rscservice import RSCFactory, load_setup
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet import reactor
from twisted.internet.protocol import Protocol

import rscoin
from base64 import b64encode, b64decode

class closeEpochMessage(Protocol):
    def sendMessage(self, msg):
        self.transport.write(msg)

def gotProtocol(p):
    p.sendMessage("xCloseEpoch")

def CloseEpoch():
    point = TCP4ClientEndpoint(reactor, "127.0.0.1", 8080, timeout=10)
    d = connectProtocol(point, closeEpochMessage())
    d.addCallback(gotProtocol)

secret = file("secret.key").read()
public = rscoin.Key(secret, public=False)
print "Public keys: %s" % b64encode(public.pub.export())

dir_data = file("directory.conf").read()
directory = load_setup(dir_data) # [(public.id(), "127.0.0.1", 8080)]

application = service.Application("rscoin")
echoService = internet.TCPServer(8080, RSCFactory(secret, directory["directory"], directory["special"], N=3))
echoService.setServiceParent(application)
ts = TimerService(60, CloseEpoch)
ts.setServiceParent(application)
