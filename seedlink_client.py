__author__ = 'jman'

from obspy.seedlink.easyseedlink import create_client, EasySeedLinkClientException
from obspy.seedlink.client.seedlinkconnection import SeedLinkException
import Queue
import threading
import logging
import time


# default logger
logger = logger = logging.getLogger('mangoC.SeedlinkClient')
LOG_PRINT_COUNT_THRESHOLD = 1

class SeedlinkClient(threading.Thread):

    def __init__(self,
                 server_name,
                 select_list,
                 queue
                 ):

        threading.Thread.__init__(self)
        self._queue = queue
        self._server_name = server_name
        self._select_list = select_list
        self._count = 0

    def on_data(self, trace):

        """print(trace.meta.network)
        print(trace.meta.station)
        print(trace.meta.location)
        print(trace.meta.channel)
        print(trace.meta.starttime)
        print(trace.meta.endtime)
        print(trace.meta.sampling_rate)
        print(trace.meta.delta)
        print(trace.meta.npts)
        print(trace.meta.calib)

        print(len(trace.data))
        print(trace.data)"""

        if self._count > LOG_PRINT_COUNT_THRESHOLD:
                logging.info('Received new data: %s.%s.%s.%s | sampling:%s | npts:%d | calib:%d | st:%s | %d' % (trace.meta.network, trace.meta.station, trace.meta.location, trace.meta.channel, trace.meta.sampling_rate, trace.meta.npts, trace.meta.calib, trace.meta.starttime, self._queue.qsize()))
                self._count = 0

        self._queue.put(trace)
        self._count += 1
        #logging.info('Received new data: %s.%s.%s.%s | sampling:%s | npts:%d | calib:%d | st:%s | %d' % (trace.meta.network, trace.meta.station, trace.meta.location, trace.meta.channel, trace.meta.sampling_rate, trace.meta.npts, trace.meta.calib, trace.meta.starttime, self._queue.qsize()))
        #print 'Received new data: %s.%s.%s.%s | sampling:%s | npts:%d | calib:%d | st:%s | %d' % (trace.meta.network, trace.meta.station, trace.meta.location, trace.meta.channel, trace.meta.sampling_rate, trace.meta.npts, trace.meta.calib, trace.meta.starttime, self._queue.qsize())

        pass

    def on_error(self):
        print('error: %s\n' % self)
        pass

    @property
    def client(self):
        # Do something if you want
        return self._client

    def run(self):

        while True:

            self._client = create_client(self._server_name, self.on_data, self.on_error)

            for line in self._select_list:
                words = line.split(",")
                self._client.select_stream(words[0], words[1], words[2])

            try:
                self._client.run()

            except EasySeedLinkClientException as exception:
                logger.error(exception.args)

                return

            except SeedLinkException as exception:
                logger.warn("Retry... ")
                time.sleep(10)
                pass
