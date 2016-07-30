from twisted.application import internet, service
from twisted.application.internet import TimerService
from rscoin.rscservice import Central_Bank, load_setup

import rscoin
from base64 import b64encode, b64decode

dir_data = file("directory.conf").read()
directory = load_setup(dir_data)
this_bank = Central_Bank(directory["directory"])
secret = file("secret.key").read()

application = service.Application("rscoin")
ts = TimerService(1, this_bank.process_lower_blocks, secret)
ts.setServiceParent(application)
