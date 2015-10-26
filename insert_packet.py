__author__ = 'jman'

import json
import threading
import logging
import pymongo
from pymongo.errors import AutoReconnect
from pymongo.errors import DuplicateKeyError
import numpy as np
from string_utils import StringUtils


# default logger
logger = logging.getLogger('mangoC.InsertPacket')
INS_PACKET_LOG_PRINT_COUNT_THRESHOLD = 500


class InsertPacket(threading.Thread):

    def __init__(self, mongo_name, db_name, queue):

        threading.Thread.__init__(self)

        self._mongo_name = mongo_name
        self._db_name = db_name
        self._queue = queue

        try:
            self._con = pymongo.MongoClient(mongo_name)
        except pymongo.errors.ConnectionFailure:
            logger.error("No Host found", exc_info=True)
            return

        self._db = self._con['trace']
        pass

    def run(self):

        self._print_cnt = 0

        while(True):
           self.insert_document_per_packet_and_station_collection_unlimited()

        pass

    def insert_document_per_packet_and_station_collection_unlimited(self):

        # get from queue
        trace = self._queue.get()

        # get collection name
        network_name = trace.meta.network
        station_name = trace.meta.station
        location_name = trace.meta.location
        channel_name = trace.meta.channel
        start_time = trace.meta.starttime
        end_time = trace.meta.endtime
        sampling_rate = trace.meta.sampling_rate
        ntps = trace.meta.npts
        calib = trace.meta.calib

        # make collection name
        collection_name_list = []
        collection_name_list.append(network_name)
        collection_name_list.append("_")
        collection_name_list.append(station_name)
        collection_name_list.append("_")
        collection_name_list.append(location_name)
        collection_name = str(''.join(collection_name_list))

        collection_name_list2 = []
        collection_name_list2.append(network_name)
        collection_name_list2.append("_")
        collection_name_list2.append(station_name)
        collection_name_list2.append("_")
        collection_name_list2.append(location_name)
        collection_name_list2.append("_")
        collection_name_list2.append(channel_name)
        collection_name2 = str(''.join(collection_name_list2))

        # make key
        str_yyyy_mm_dd_hh_mm_ss = '{0:04d}'.format(start_time._getYear()) + "-" + '{0:02d}'.format(start_time._getMonth()) + "-" + '{0:02d}'.format(start_time._getDay()) + "T" + '{0:02d}'.format(start_time._getHour()) + ':' + '{0:02d}'.format(start_time._getMinute()) + ':' + '{0:02d}'.format(start_time._getSecond())

        # data
        data = {}
        data['st'] = str(start_time)
        data['et'] = str(end_time)
        data['n'] = ntps
        data['c'] = calib
        data['s'] = sampling_rate
        data['d'] = trace.data.astype(np.int32).tolist()

        # key string
        key = {}
        key['_id'] = str_yyyy_mm_dd_hh_mm_ss

        # insert mongoDB
        self._col = self._db[collection_name]

        try:
            self._col.update(key, {'$set': {channel_name:data}}, upsert=True,multi=False)

            # make stats collection
            stat_key = {}
            stat_key['_id'] = collection_name2
            stat_collection = self._db['TRACE_STATS']
            stat_collection.update(stat_key, {"et": data['et']}, upsert=True, multi=False)

            # make index
            self._col.ensure_index(channel_name + ".st", background=True)
            self._col.ensure_index(channel_name + ".et", background=True)

            self._print_cnt += 1
            if self._print_cnt > INS_PACKET_LOG_PRINT_COUNT_THRESHOLD:
                logger.info("Insert data. %s.%s.%s.%s | st:%s| %d", network_name, station_name, location_name, channel_name, start_time, self._queue.qsize())

            self._print_cnt = 0

        except AutoReconnect as exception:
            logger.warn(exception.args)
            pass

        except DuplicateKeyError as exception:

            self._col.update(key, {'$set':{channel_name:data}}, upsert=False,multi=False)
            logger.info(">> Update data. %s.%s.%s.%s | st:%s| %d", network_name, station_name, location_name, channel_name, start_time, self._queue.qsize())
            pass


        pass
