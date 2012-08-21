#/usr/bin/env python3

import threading
import logging
import queue
import subprocess
import re
import time
import math


logging.basicConfig(level=logging.INFO,
                    format='%(threadName)-10s: %(asctime)s [%(levelname)-8s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M',)

# queue containing all unknown/unlabeled tapes
tapes_queue = queue.Queue()

# number of drives to use
num_threads = 4

# names of bacula's pools to use
pools = ['Full', 'Diff', 'Inc']


class BaculaCommandJob(threading.Thread):

    def __init__(self, drive=0, action=None):
        threading.Thread.__init__(self)
        logging.debug('%s started' % self.name)
        self.drive = drive
        self.action = action

    def labelTape(self):
        while not tapes_queue.empty():
            job = tapes_queue.get()
            logging.info('Starting label tape %s in slot %s on drive-%d' % (job['tape'], job['slot'], self.drive))

            cmd = '''bconsole << END_OF_DATA
@output /tmp/import-drive-%(drive)d.log
@time
label barcodes slots=%(slot)s drive=%(drive)d pool=%(pool)s
yes
wait
quit
END_OF_DATA''' % {'drive': self.drive, 'slot': job['slot'], 'pool': job['pool']}

            self.cmd(cmd)
            time.sleep(1)
        logging.debug('%s finished' % self.name)

    def releaseDrive(self):
        logging.info('Releasing drive-%d' % self.drive)
        self.cmd('echo "release drive=%d" | bconsole -s -n' % self.drive)
        logging.info('drive-%d released' % self.drive)

    def cmd(self, cmd):
        logging.debug('Running command: %s' % cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        return p.communicate()[0].decode('UTF8').strip()

    def run(self):
        if self.action:
            method = getattr(self, self.action)
            method()


def getUnknownTapes():
    logging.info('Searching for unlabed tapes')

    b = BaculaCommandJob()
    b.start()

    cmd = 'echo "update slots drive=0" | bconsole -s -n | grep "not found in catalog"'
    output = b.cmd(cmd)
    if output == "":
        logging.info('No unlabled tapes found')
        return False
    lines = output.split('\n')

    # multiply pools list to the size of the tapes list and insert into queue.
    # Thus, new tapes will be distributed to all available pools
    pools_t = pools * math.ceil(len(lines) / len(pools))

    pattern = r'Volume "(?P<tape>.*)" .* Slot=(?P<slot>\d+) .*'
    for l in lines:
        m = re.match(pattern, l)
        jobInfo = m.groupdict()
        jobInfo.update(dict({'pool': pools_t.pop()}))
        tapes_queue.put(jobInfo)
    b.join()
    return True


def labelTapes():
    if tapes_queue.empty():
        logging.error('No unlabled tapes in queue')
        return

    for i in range(num_threads):
        b = BaculaCommandJob(i, 'labelTape')
        b.start()

    logging.info('All worker threads started, waiting for finished action queue ...')

    # wait until the queue is emptied
    while not tapes_queue.empty():
        time.sleep(1)

    logging.info('Queue is empty. Waiting for threads to finish.')


def waitForThreads():
    while threading.activeCount() > 1:
        time.sleep(1)


def releaseTapesFromDrives():
    for i in range(num_threads):
        b = BaculaCommandJob(i, 'releaseDrive')
        b.start()

    waitForThreads()


releaseTapesFromDrives()
if getUnknownTapes() is True:
    labelTapes()


waitForThreads()

logging.info('All threads finished. Closing Main Thread.')
