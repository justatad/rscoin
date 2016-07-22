from twisted.application import internet, service
from twisted.application.internet import TimerService
from rscoin.rscservice import Central_Bank

import rscoin
from base64 import b64encode, b64decode

this_bank = Central_Bank()

application = service.Application("rscoin")
ts = TimerService(5, this_bank.process_lower_blocks)
ts.setServiceParent(application)
