__author__ = 'jman'

import Queue
import threading
import logging.config
import os
import yaml
import time
from seedlink_client import SeedlinkClient
from insert_packet import InsertPacket

def main():

    #log
    logger = logging.getLogger('mangoS')
    loggingConfigPath = 'logging.yaml'
    if os.path.exists(loggingConfigPath):
        with open(loggingConfigPath, 'rt') as f:
            loggingConfig = yaml.load(f.read())
            logging.config.dictConfig(loggingConfig)
    else:
        logger.basicConfig(level=logging.INFO)

    #variables
    server_name = "rtserve.iris.washington.edu:18000"
    mongo_name = "localhost"
    db_name = "trace"

    queue = Queue.Queue()

    mongo_name = "mongodb://" + mongo_name
    ins_packet = InsertPacket(mongo_name, db_name, queue)
    ins_packet.start()

    ins_packet2 = InsertPacket(mongo_name, db_name, queue)
    ins_packet2.start()

    ins_packet3 = InsertPacket(mongo_name, db_name, queue)
    ins_packet3.start()

    ins_packet4 = InsertPacket(mongo_name, db_name, queue)
    ins_packet4.start()

    ins_packet5 = InsertPacket(mongo_name, db_name, queue)
    ins_packet5.start()

    select_list = []
    select_list.append("AK,ANM,???")

    sc_client = SeedlinkClient(server_name, select_list, queue)
    sc_client.start()

    pass

if __name__ == "__main__":
    main()

