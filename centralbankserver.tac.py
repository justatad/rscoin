from twisted.application import internet, service
from twisted.application.internet import TimerService
from rscoin.rscservice import RSCFactory, load_setup

import rscoin
from base64 import b64encode, b64decode


application = service.Application("rscoin")
ts = TimerService(60, Central_Bank.print_message())
ts.setServiceParent(application)
