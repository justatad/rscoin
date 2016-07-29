from base64 import b64encode, b64decode
from binascii import hexlify
from struct import pack, unpack

from json import loads
from bisect import bisect_left
# import bsddb

from future.moves import dbm

from traceback import print_stack, print_exc
from hashlib import sha256
from os.path import join

from petlib.ec import EcPt

from twisted.internet import protocol
from twisted.protocols.basic import LineReceiver
from twisted.python import log

from hippiehug import DocChain, Tree

from hotqueue import HotQueue

import time

import rscoin

def load_setup(setup_data):
    structure = loads(setup_data)
    structure["special"] = b64decode(structure["special"])
    structure["directory"] = [(b64decode(a), b, c) for a,b,c in structure["directory"]]

    return structure


def package_query(tx, tx_deps, keys):

    items = [ tx.serialize() ]
    for txi in tx_deps:
        items += [ txi.serialize() ]

    for k in keys:
        items += [ k.export()[0] ]

    for k in keys:
        items += [ k.sign(tx.id()) ]

    dataCore = map(b64encode, items)

    H = sha256(" ".join(dataCore)).digest()
    data = " ".join(["xQuery", str(len(dataCore))] + dataCore)

    return H, data, dataCore


def unpackage_query_response(response):
    resp = response.strip().split(" ")

    code = resp[0]
    if code == "OK" or code == "Pong":
        resp[1:] = map(b64decode, resp[1:])

    return resp


def package_commit(core, ks_list):
    ks_flat = []
    for (k, s) in ks_list:
        ks_flat += [ k, s ]

    data = " ".join(["xCommit", str(len(core))] + core + map(b64encode, ks_flat))
    return data


def package_issue(tx, ks):
    tx_ser = tx.serialize()
    k, s = ks
    core = map(b64encode, [tx_ser, k.export()[0], s])
    data = " ".join(["xCommit", str(len(core))] + core)
    return data


def unpackage_commit_response(response):
    resp = response.strip().split(" ")

    code = resp[0]
    if code == "OK" or code == "Pong":
        resp[1:] = map(b64decode, resp[1:])

    return resp


def unpackage_period_response(response):
    resp = response.strip().split(" ")

    code = resp[0]
    if code == "OK":
        resp[1:] = map(b64decode, resp[1:])

    return resp


class RSCProtocol(LineReceiver):

    def __init__(self, factory):
        self.factory = factory

    @staticmethod
    def parse_Tx_bundle(bundle_items, items):
        """ Common parsing code for the Tx bundle """

        assert len(items) == bundle_items
        H = sha256(" ".join(items)).digest()

        items = map(b64decode, items)
        mainTx = rscoin.Tx.parse(items.pop(0))

        otherTx, keys, sigs = [], [], []
        if len(mainTx.inTx) > 0:
                    size = len(mainTx.inTx)
                    for _ in range(size):
                        otherTx += [items.pop(0)]
                    for _ in range(size):
                        keys += [items.pop(0)]
                    for _ in range(size):
                        sigs += [items.pop(0)]
        elif len(mainTx.inTx) == 0:
                    keys += [items.pop(0)]
                    sigs += [items.pop(0)]

        assert len(items) == 0

        return H, (mainTx, otherTx, keys, sigs)


    def handle_Query(self, items):
        """ Process the query message and respond """
        bundle_size = int(items[1])

        try:
            items = items[2:2+bundle_size]
            H, data = RSCProtocol.parse_Tx_bundle( bundle_size, items)
            (mainTx, otherTx, keys, sigs) = data

            # Specific checks
            assert len(otherTx) > 0
            assert len(items[2+bundle_size:]) == 0

            # TODO: checkhere this Tx falls within our remit

        except Exception as e:
            print_exc()
            self.return_Err("ParsingError")
            return

        try:
            # Check the Tx Query
            res = self.factory.process_TxQuery(data)
        except Exception as e:
            print_exc()
            self.return_Err("QueryError")
            return

        # If Query failed
        if not res:
            self.sendLine("NOTOK" )
            return

        self.sendLine("OK %s" % self.sign(H))
        return

    def handle_Commit(self, items):
        """ Process the commit message and respond """

        try:
            bundle_size = int(items[1])

            extras = items[2+bundle_size:]
            items = items[2:2+bundle_size]
            H, data = RSCProtocol.parse_Tx_bundle( bundle_size, items)
            (mainTx, otherTx, keys, sigs) = data

            # Specific checks
            assert len(items[2+bundle_size:]) == 0

            auth_keys, auth_sigs = [], []
            while len(extras) > 0:
                auth_keys += [ b64decode(extras.pop(0)) ]
                auth_sigs += [ b64decode(extras.pop(0)) ]

            assert len(extras) == 0
        except:
            print_exc()
            self.return_Err("ParsingError")
            return

        data = (H, mainTx, otherTx, keys, sigs, auth_keys, auth_sigs)
        res = self.factory.process_TxCommit(data)

        if not res:
            self.sendLine("NOTOK")
            return

        h = mainTx.id()
        ret = self.sign(h)
        self.sendLine("OK %s" % ret)


    def handle_ClosePeriod(self):
        """ Process the close period message and respond """

        try:
            self.factory.periodStatus = 'Closed'
        except:
            self.sendLine("NOTOK")
            print_exc()
            self.return_Err("PeriodCloseError")
            return

        self.sendLine("OK")


    def handle_OpenPeriod(self, items):
        """ Process the open period message and respond """

        try:
            self.factory.periodStatus = 'Open'
            self.factory.lastHigherBlockHash = items[1]
        except:
            self.sendLine("NOTOK")
            print_exc()
            self.return_Err("PeriodOpenError")
            return

        self.sendLine("OK")


    def lineReceived(self, line):
        """ Simple de-multiplexer """

        items = line.split(" ")
        if items[0] == "xQuery":
            return self.handle_Query(items) # Get signatures

        if items[0] == "xCommit":
            return self.handle_Commit(items) # Seal a transaction

        if items[0] == "Ping":
            self.sendLine("Pong %s" % b64encode(self.factory.key.id()))
            return # self.handle_Commit(items) # Seal a transaction

        if items[0] == "xClosePeriod":
            return self.handle_ClosePeriod() # Close period on mintette

        if items[0] == "xOpenPeriod":
            return self.handle_OpenPeriod(items) # Open new period on mintette

        self.return_Err("UnknownCommand:%s" % items[0])
        return

    def sign(self, H):
        """ Generic signature """
        k = self.factory.key
        pub = k.pub.export(EcPt.POINT_CONVERSION_UNCOMPRESSED)
        sig = k.sign(H)
        return " ".join(map(b64encode, [pub, sig]))


    def return_Err(self, Err):
        self.sendLine("Error %s" % Err)


class RSCFactory(protocol.Factory):

    _sync = False

    def __init__(self, secret, directory, special_key, conf_dir=None, N=3):
        """ Initialize the RSCoin server"""
        self.special_key = special_key
        self.key = rscoin.Key(secret, public=False)
        self.directory = sorted(directory)
        self.keyID = self.key.id()[:10]
        self.kid = b64encode(self.key.id())
        self.N = N
        self.txCount = 0
        self.txset_tree = Tree()
        self.mset = set()
        self.txset = set()
        self.lastLowerBlockHash = ''
        self.lastHigherBlockHash = ''
        self.periodStatus = 'Open'
        self.epochId = 0

        # Connect to Redis
        self.queue = HotQueue("rscoin", host="rscoinredis.p1h0i7.0001.euw1.cache.amazonaws.com", port=6379, db=0)

        # Open the databases
        self.dbname = 'keys-%s' % hexlify(self.keyID)
        self.logname = 'log-%s' % hexlify(self.keyID)

        if conf_dir:
            self.dbname = join(conf_dir, self.dbname)
            self.logname = join(conf_dir, self.logname)

        if RSCFactory._sync:
            self.db = dbm.open(self.dbname, 'c')
            self.log = dbm.open(self.logname, 'c')
        else:
            self.db = {}
            self.log = {}


    def buildProtocol(self, addr):
        cli = RSCProtocol(self)
        return cli


    def process_TxQuery(self, data):
        """ Queries a full transaction and gets a signed response if it is valid.

            When I get a query:
            * Check that the signatures check.
            * Check the input addresses are in the utxo.
            * Check that they are not used, or used for same.
            * Add to spent transactions.
            * Remove from utxo.

        """

        mainTx, otherTx, keys, sigs = data
        mid = mainTx.id()
        inTxo = mainTx.get_utxo_in_keys()

        # Check that at least one Input is handled by this server
        should_handle_ik = []
        for ik in inTxo:
            lst = self.get_authorities(ik)
            if self.key.id() in lst:
                should_handle_ik += [ ik ]

        if should_handle_ik == []:
            #print("Not in ID range.")
            return False

        # Check the transaction is well formed
        all_good = mainTx.check_transaction(otherTx, keys, sigs)
        if not all_good:
            print("Failed TX check")
            return False

        ## Check all inputs are in
        for ik in should_handle_ik:
            ## We are OK if this is already in the log with the same mid
            if ik in self.log and self.log[ik] == mid:
                continue
            ## Otherwise it should not have been used yet
            elif ik not in self.db:
                print b64encode(ik)[:8]
                print("Failed utxo check")
                return False

        # Once we know all is good we proceed to remove them
        # but write the decision to a log
        for ik in should_handle_ik:
            if ik in self.db:
                del self.db[ik]
            self.log[ik] = mid

        # Save before signing
        if RSCFactory._sync:
            self.db.sync()
            self.log.sync()
        return True

    def process_TxCommit(self, data):
        """ Provides a Tx and a list of responses, and commits the transaction. """
        H, mainTx, otherTx, keys, sigs, auth_pub, auth_sig = data
        mset_output = ''
        txset_output = ''

        # Check that this Tx is handled by this server
        ik = mainTx.id()
        lst = self.get_authorities(ik)
        should_handle = (self.key.id() in lst)

        if not should_handle:
            log.msg('Failed Tx ID range ownership')
            return False

        # First check all signatures
        all_good = True
        pub_set = []
        for pub, sig in zip(auth_pub, auth_sig):
            key = rscoin.Key(pub)
            pub_set += [ key.id() ]
            all_good &= key.verify(H, sig)

        if not all_good:
            log.msg('Failed Tx Signatures')
            return False

        # Check the transaction is well formed
        all_good = mainTx.check_transaction(otherTx, keys, sigs, masterkey=self.special_key)
        if not all_good:
            log.msg('Failed Tx Validity')
            return False

        pub_set = set(pub_set)

        # Now check all authorities are involved
        mid = mainTx.id()
        inTxo = mainTx.get_utxo_in_keys()
        for itx in inTxo:
            ## Ensure we have a Quorum for each input
            aut = set(self.get_authorities(itx))
            all_good &= (len(aut & pub_set) > len(aut) / 2)

        if not all_good:
            log.msg('Failed Tx Authority Consensus')
            return False

        # Now check we have not already spent the transaction
        all_good &= (mid not in self.log)

        if not all_good:
            log.msg('Failed Tx Doublespending')
            return False

        # Update the outTx entries
        for k, v in mainTx.get_utxo_out_entries():
            self.db[k] = v

        if RSCFactory._sync:
            self.db.sync()

        # Store information used to generate the lower level block for this mintette
        self.txCount += 1
        if len(otherTx) >= 1:
            self.mset |= set(otherTx)
        self.txset |= set(mainTx.id())

        # Check to see if enough transactions have been received and close the epoch if they have
        if self.txCount >= 100 and self.periodStatus == 'Open':
            self.epochId += 1

        if len(self.mset) == 0:
            mset_output = ''
            if len(self.mset) == 1:
                mset_output = b64encode(self.mset)
            if  len(self.mset) > 1:
                mset_output += " ".join([b64encode(str(i)) for i in self.mset])
            for i in self.txset:
                self.txset_tree.add(b64encode(i))

            if len(self.txset) == 1:
                txset_output = b64encode(self.txset)
            if len(self.txset) > 1:
                mset_output += " ".join([b64encode(str(i)) for i in self.txset])

            H = sha256(self.lastHigherBlockHash + self.lastLowerBlockHash + mset_output + self.txset_tree.root()).digest()
            lower_block = (H, txset_output, self.sign(H), memset_output, self.kid, self.epochId, b64encode(self.lastHigherBlockHash), b64encode(self.lastLowerBlockHash), b64encode(self.txset_tree.root()))
            self.queue.put(lower_block)

            self.lastLowerBlockHash = H
            self.txCount = 0
            self.txset_tree = Tree()
            self.mset = set()
            self.txset = set()

        return all_good


    def get_authorities(self, xID):
        """ Returns the keys of the authorities for a certain xID """
        return get_authorities(self.directory, xID, self.N)


    def sign(self, H):
        """ Generic signature """
        k = self.key
        pub = k.pub.export(EcPt.POINT_CONVERSION_UNCOMPRESSED)
        sig = k.sign(H)
        return " ".join(map(b64encode, [pub, sig]))


def get_authorities(directory, xID, N = 3):
    """ Returns the keys of the authorities for a certain xID """
    d = sorted(directory)

    if __debug__:
        for di, _, _ in d:
            assert isinstance(di, str) and len(di) == 32

    if len(d) <= N:
        auths = [di[0] for di in d]
    else:
        i = unpack("I", xID[:4])[0] % len(d)
        # i = bisect_left(d, (xID, None, None))
        auths =  [d[(i + j - 1) % len(d)][0] for j in range(N)]


    assert 0 <= len(auths) <= N
    # print N
    return auths


class Central_Bank:

    def __init__(self, directory):
        # Connected to Redis for lower level blocks
        self.queue = HotQueue("rscoin", host="rscoinredis.p1h0i7.0001.euw1.cache.amazonaws.com", port=6379, db=0)
        self.start_time = time.time()
        self.mintette_hashes = dict()
        self.lastHigherBlockHash = ''
        self.lower_blocks = []
        self.period_txset = set()
        dir = [(kid, ip, port) for (kid, ip, port) in directory]
        for (kid, ip, port) in dir:
            self.mintette_hashes[(b64encode(kid))] = ''
        self.dir = [(kid, ip, port) for (kid, ip, port) in directory]
        self.central_bank_chain = DocChain()


    def broadcast(small_dir, data):
        d_list = []
        responses = []
        # d = defer.Deferred()

        def gotProtocol(p):
            p.sendLine(data)

        for (kid, ip, port) in small_dir:
            _stats[ip] += 1
            point = TCP4ClientEndpoint(reactor, ip, int(port), timeout=10)
            f = RSCfactory()

            d = point.connect(f)
            d.addCallback(gotProtocol)
            d.addErrback(f.d.errback)

            d_list += [ f.d ]

        d_all = defer.gatherResults(d_list)
        return d_all


    def get_period_responses(resp):
        try:
            assert len(resp) <= 3
            for r in resp:
                res = unpackage_epoch_response(r)
                if res[0] != "OK":
                    print resp
                    d_end.errback(Exception("Period notification failed."))
                    return
                print "Period OK"
                d_end.callback()
        except Exception as e:
            d_end.errback(e)
            return


    def restart_time(self):
        self.start_time = time.time()


    def validate_lower_block(self, lower_block):
        all_good = True
        H_mintette, txset, sig, mset, mintette_id, epochId, lastHigherBlockHash, lastLowerBlockHash, txset_tree_root = lower_block

        # Validate the sig of the lower block from the mintette
        sig_elements = sig.split(" ")
        key = rscoin.Key(b64decode(sig_elements[0]))
        all_good &= key.verify(H_mintette, b64decode(sig_elements[1]))
        if not all_good:
            log.msg("Signatue verification failed for lower level block from mintette %s" % mintette_id)
            return False

        # Validate that the hash of the elements in the lower matches the hash value accompanying them
        txset_tree = Tree()
        txset_list = txset.split(" ")
        for i in txset_list:
            txset_tree.add(i)
        H = sha256(self.lastHigherBlockHash + self.mintette_hashes[mintette_id] + mset + txset_tree.root()).digest()
        if H_mintette != H:
            log.msg("Lower block hash not valid from mintette %s" % mintette_id)
	    log.msg(b64encode(self.lastHigherBlockHash))
            log.msg(lastHigherBlockHash)
            log.msg(b64encode(self.mintette_hashes[mintette_id]))
            log.msg(lastLowerBlockHash)
            log.msg(b64encode(txset_tree.root()))
            log.msg(txset_tree_root)
            log.msg(epochId)
            return False

        return all_good


    def process_lower_blocks(self):
        if time.time() - self.start_time > 60:
            # Period has ended, notify mintettes so they stop sending lower level blocks for this period
            log.msg('Period now ending')
            p_msg = "xClosePeriod"
            d = broadcast(self.dir, p_msg)
            d.addCallback(get_period_responses)
            d.addErrback(d_end.errback)

            if len(self.period_txset) != 0:
                period_txset_tree = Tree()
                for i in self.period_txset:
                    period_txset_tree.add(i)
                H = sha256(self.central_bank_chain.root() + period_txset_tree.root()).digest()
                sig = self.sign(H)
                self.central_bank_chain.multi_add(H, sig)

            log.msg('Opening new period')
            p_msg = "xOpenPeriod %s" % b64encode(self.central_bank_chain.root())
            d = broadcast(self.dir, p_msg)
            d.addCallback(get_period_responses)
            d.addErrback(d_end.errback)

            self.restart_time()


        lower_block = self.queue.get()

        if self.validate_lower_block(lower_block) == True:
                log.msg('Lower block valid')
                #self.lower_blocks += lower_block
                self.period_txset |= set(lower_block[1])
                self.mintette_hashes[(b64encode(lower_block[4]))] = lower_block[0]
                message.delete()
