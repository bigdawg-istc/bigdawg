#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from pyaccumulo.proxy import AccumuloProxy
from pyaccumulo.proxy.ttypes import ScanColumn, ColumnUpdate, ScanOptions, Key, BatchScanOptions, TimeType, WriterOptions, IteratorSetting
import pyaccumulo.proxy.ttypes

from collections import namedtuple
from pyaccumulo.iterators import BaseIterator

from array import array

Cell = namedtuple("Cell", "row cf cq cv ts val")

def _get_scan_columns(cols):
    columns = None
    if cols:
        columns = []
        for col in cols:
            sc = ScanColumn()
            sc.colFamily = col[0]
            sc.colQualifier = col[1] if len(col) > 1 else None
            columns.append(sc)
    return columns

def following_array(val):
    if val:
        return val+"\0"
    else:
        return None

def following_key(key):
    """
    Returns the key immediately following the input key - based on the Java implementation found in
    org.apache.accumulo.core.data.Key, function followingKey(PartialKey part)
    :param key: the key to be followed
    :return: a key that immediately follows the input key
    """
    if key.timestamp is not None:
        key.timestamp -= 1
    elif key.colVisibility is not None:
        key.colVisibility = following_array(key.colVisibility)
    elif key.colQualifier is not None:
        key.colQualifier = following_array(key.colQualifier)
    elif key.colFamily is not None:
        key.colFamily = following_array(key.colFamily)
    elif key.row is not None:
        key.row = following_array(key.row)
    return key

class Mutation(object):
    def __init__(self, row):
        super(Mutation, self).__init__()
        self.row = row
        self.updates = []

    def put(self, cf='', cq='', cv=None, ts=None, val='', is_delete=None):
        self.updates.append(ColumnUpdate(colFamily=cf, colQualifier=cq, colVisibility=cv, timestamp=ts, value=val, deleteCell=is_delete))

class Range(object):
    def __init__(self, 
                 srow=None, scf=None, scq=None, scv=None, sts=None, sinclude=True,
                 erow=None, ecf=None, ecq=None, ecv=None, ets=None, einclude=True):

        super(Range, self).__init__()

        self.srow = srow
        self.scf = scf
        self.scq = scq
        self.scv = scv
        self.sts = sts
        self.sinclude = sinclude

        self.erow = erow
        self.ecf = ecf
        self.ecq = ecq
        self.ecv = ecv
        self.ets = ets
        self.einclude = einclude

    @staticmethod
    def followingPrefix(prefix):
        """Returns a String that sorts just after all Strings beginning with a prefix"""
        prefixBytes = array('B', prefix)

        changeIndex = len(prefixBytes) - 1
        while (changeIndex >= 0 and prefixBytes[changeIndex] == 0xff ):
            changeIndex = changeIndex - 1;
        if(changeIndex < 0):
            return None
        newBytes = array('B', prefix[0:changeIndex + 1])
        newBytes[changeIndex] = newBytes[changeIndex] + 1
        return newBytes.tostring()

    @staticmethod
    def prefix(rowPrefix):
        """Returns a Range that covers all rows beginning with a prefix"""
        fp = Range.followingPrefix(rowPrefix)
        return Range(srow=rowPrefix, sinclude=True, erow=fp, einclude=False)


    def to_range(self):
        r = proxy.ttypes.Range()
        r.startInclusive = self.sinclude
        r.stopInclusive = self.einclude

        if self.srow:
            r.start = Key(row=self.srow, colFamily=self.scf, colQualifier=self.scq, colVisibility=self.scv, timestamp=self.sts)
            if not self.sinclude:
                r.start = following_key(r.start)
            
        if self.erow:
            r.stop = Key(row=self.erow, colFamily=self.ecf, colQualifier=self.ecq, colVisibility=self.ecv, timestamp=self.ets)
            if self.einclude:
                r.stop = following_key(r.stop)
        
        return r


class BatchWriter(object):
    """docstring for BatchWriter"""
    def __init__(self, conn, table, max_memory=10*1024, latency_ms=30*1000, timeout_ms=5*1000, threads=10):
        super(BatchWriter, self).__init__()
        self._conn = conn
        self._writer = conn.client.createWriter(self._conn.login, table, WriterOptions(maxMemory=max_memory, latencyMs=latency_ms, timeoutMs=timeout_ms, threads=threads))
        self._is_closed = False

    ''' muts - a list of Mutation objects '''
    def add_mutations(self, muts):
        if self._is_closed:
            raise Exception("Cannot write to a closed writer")

        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        self._conn.client.update(self._writer, cells)

    ''' mut - a Muation object '''
    def add_mutation(self, mut):
        if self._is_closed:
            raise Exception("Cannot write to a closed writer")
        self._conn.client.update(self._writer, {mut.row: mut.updates})

    def flush(self):
        if self._is_closed:
            raise Exception("Cannot flush a closed writer")
        self._conn.client.flush(self._writer)

    def close(self):
        self._conn.client.closeWriter(self._writer)
        self._is_closed = True

class Accumulo(object):
    """ Proxy Accumulo """
    def __init__(self, host="localhost", port=50096, user='root', password='secret', _connect=True):
        super(Accumulo, self).__init__()
        self.transport = TTransport.TFramedTransport(TSocket.TSocket(host, port))
        self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
        self.client = AccumuloProxy.Client(self.protocol)

        if _connect:
            self.transport.open()
            self.login = self.client.login(user, {'password':password})

    def close(self):
        self.transport.close()

    def list_tables(self):
        return [t for t in self.client.listTables(self.login)]

    def table_exists(self, table):
        return self.client.tableExists(self.login, table)

    def create_table(self, table):
        self.client.createTable(self.login, table, True, TimeType.MILLIS)

    def delete_table(self, table):
        self.client.deleteTable(self.login, table)

    def rename_table(self, oldtable, newtable):
        self.client.renameTable(self.login, oldtable, newtable)

    def write(self, table, muts):
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]

        writer = self.create_batch_writer(table)
        writer.add_mutations(muts)
        writer.close()

    def _get_range(self, scanrange):
        if scanrange:
            return scanrange.to_range()
        else:
            return None

    def _get_ranges(self, scanranges):
        if scanranges:
            return [scanrange.to_range() for scanrange in scanranges]
        else:
            return None

    def _get_iterator_settings(self, iterators):
        if not iterators: return None
        return [ self._process_iterator(i) for i in iterators ]

    def _process_iterator(self, iter):
        if isinstance(iter, IteratorSetting):
            return iter
        elif isinstance(iter, BaseIterator):
            return iter.get_iterator_setting()
        else:
            raise Exception("Cannot process iterator: %s"%iter)

    def scan(self, table, scanrange=None, cols=None, auths=None, iterators=None, bufsize=None, batchsize=10):
        options = ScanOptions(auths, self._get_range(scanrange), _get_scan_columns(cols), self._get_iterator_settings(iterators), bufsize)
        scanner = self.client.createScanner(self.login, table, options)
        return self.perform_scan(scanner, batchsize)

    def batch_scan(self, table, scanranges=None, cols=None, auths=None, iterators=None, numthreads=None, batchsize=10):
        options = BatchScanOptions(auths, self._get_ranges(scanranges), _get_scan_columns(cols), self._get_iterator_settings(iterators), numthreads)
        scanner = self.client.createBatchScanner(self.login, table, options)
        return self.perform_scan(scanner, batchsize)

    def perform_scan(self, scanner, batchsize):
        while True:
            results = self.client.nextK(scanner, batchsize)
            for e in results.results:
                yield Cell(e.key.row, e.key.colFamily, e.key.colQualifier, e.key.colVisibility, e.key.timestamp, e.value)
        
            if not results.more:
                self.client.closeScanner(scanner)
                raise StopIteration
    
    def create_batch_writer(self, table, max_memory=10*1024, latency_ms=30*1000, timeout_ms=5*1000, threads=10):
        return BatchWriter(self, table, max_memory, latency_ms, timeout_ms, threads)
    
    def delete_rows(self, table, srow, erow):
        self.client.deleteRows(self.login, table, srow, erow)

    def attach_iterator(self, table, setting, scopes):
        self.client.attachIterator(self.login, table, setting, scopes)

    def remove_iterator(self, table, iterator, scopes):
        self.client.removeIterator(self.login, table, iterator, scopes)

    def following_key(self, key, part):
        return self.client.getFollowing(key, part)

    def get_max_row(self, table, auths=None, srow=None, sinclude=None, erow=None, einclude=None):
        return self.client.getMaxRow(self.login, table, auths, srow, sinclude, erow, einclude)

    def add_mutations_and_flush(self, table, muts):
        """
        Add mutations to a table without the need to create and manage a batch writer.
        """
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        self.client.updateAndFlush(self.login, table, cells)

    def create_user(self, user, password):
        self.client.createLocalUser(self.login, user, password)

    def drop_user(self, user):
        self.client.dropLocalUser(self.login, user)

    def list_users(self):
        return self.client.listLocalUsers(self.login)

    def set_user_authorizations(self, user, auths):
        self.client.changeUserAuthorizations(self.login, user, auths)

    def get_user_authorizations(self, user):
        return self.client.getUserAuthorizations(self.login, user)

    def grant_system_permission(self, user, perm):
        self.client.grantSystemPermission(self.login, user, perm)

    def revoke_system_permission(self, user, perm):
        self.client.revokeSystemPermission(self.login, user, perm)

    def has_system_permission(self, user, perm):
        return self.client.hasSystemPermission(self.login, user, perm)

    def grant_table_permission(self, user, table, perm):
        self.client.grantTablePermission(self.login, user, table, perm)

    def revoke_table_permission(self, user, table, perm):
        self.client.revokeTablePermission(self.login, user, table, perm)

    def has_table_permission(self, user, table, perm):
        return self.client.hasTablePermission(self.login, user, table, perm)

    def add_splits(self, table, splits):
        self.client.addSplits(self.login, table, splits)

    def add_constraint(self, table, class_name):
        return self.client.addConstraint(self.login, table, class_name)

    def list_constraints(self, table):
        return self.client.listConstraints(self.login, table)

    def remove_constraint(self, table, constraint):
        """
        :param table: table name
        :param constraint: the constraint number as returned by list_constraints
        """
        self.client.removeConstraint(self.login, table, constraint)
