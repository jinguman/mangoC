__author__ = 'jman'

class StringUtils():

    def __init__(self):
        pass

    def get_collection_name(self, meta):

        network_name = meta.network
        station_name = meta.station
        location_name = meta.location
        channel_name = meta.channel
        start_time = meta.starttime
        end_time = meta.endtime
        sampling_rate = meta.sampling_rate

        # make collection name
        collection_name_list = []
        collection_name_list.append(network_name)
        collection_name_list.append("_")
        collection_name_list.append(station_name)
        collection_name_list.append("_")
        collection_name_list.append(location_name)
        collection_name_list.append("_")
        collection_name_list.append(channel_name)
        collection_name_list.append("_")
        collection_name_list.append(str(start_time._get_year()))
        collection_name_list.append(str(start_time._get_julday()))

        collection_name = str(''.join(collection_name_list))

        return collection_name

    def get_NSL_collection_name(self, meta):

        network_name = meta.network
        station_name = meta.station
        location_name = meta.location

        # make collection name
        collection_name_list = []
        collection_name_list.append(network_name)
        collection_name_list.append("_")
        collection_name_list.append(station_name)
        collection_name_list.append("_")
        collection_name_list.append(location_name)

        collection_name = str(''.join(collection_name_list))
        return collection_name


    def get_collection_name(self, network_name, station_name, location_name, channel_name, start_time, end_time):

        # make collection name
        collection_name_list = []
        collection_name_list.append(network_name)
        collection_name_list.append("_")
        collection_name_list.append(station_name)
        collection_name_list.append("_")
        collection_name_list.append(location_name)
        collection_name_list.append("_")
        collection_name_list.append(channel_name)
        collection_name_list.append("_")
        collection_name_list.append(str(start_time._getYear()))
        collection_name_list.append(str(start_time._getJulday()))

        _hh = int(int(start_time._getHour()) /12)
        collection_name_list.append("_")
        collection_name_list.append(str(_hh))

        collection_name = str(''.join(collection_name_list))


        return collection_name

    # make static method
    getCollectionName = classmethod(get_collection_name)