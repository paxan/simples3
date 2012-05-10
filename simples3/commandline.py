from __future__ import absolute_import, print_function

import os
from itertools import imap, izip_longest, repeat, takewhile
from operator import attrgetter

from .bucket import S3Bucket

__all__ = 'BatchDeleter', 'rm_tool'


class BatchDeleter(object):
    def __init__(self, bucket, prefix, dry_run=True):
        self.bucket = bucket
        self.prefix = prefix
        self.dry_run = dry_run

    def check_key(self, item):
        if item:
            assert item.key.startswith(self.prefix), ("%(key)s doesn't have prefix %(prefix)s"
                                                      % dict(key=item.key, prefix=self.prefix))
            return True

    def __call__(self, items):
        victims = tuple(takewhile(self.check_key, items))
        if victims and not self.dry_run:
            self.bucket.delete(*map(attrgetter('key'), victims))
        return victims

def list_matching_items(bucket, prefix):
    return takewhile(lambda i: i.key.startswith(prefix), bucket.listdir(prefix))

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    return izip_longest(fillvalue=fillvalue, *repeat(iter(iterable), n))

def rm_tool():
    bucket = S3Bucket('what.bucket.you.want.to.clean.up.today.eh',
                      access_key=os.environ['AWS_ACCESS_KEY_ID'],
                      secret_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    prefix = ''
    dry_run = True

    #from multiprocessing import Pool
    #pool = Pool(12)
    #mapper = pool.imap_unordered
    mapper = imap

    n = 0
    for deleted in mapper(BatchDeleter(bucket, prefix, dry_run),
                          grouper(1000, list_matching_items(bucket, prefix))):
        n += len(deleted)
        print("Would delete" if dry_run else "Deleted", len(deleted), "keys.")
        print("Keys processed so far:", n)
