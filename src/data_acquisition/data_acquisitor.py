import time
import pandas
from abc import ABC

import requests

from src.data_acquisition.data_transformer import DataTransformer, YouBikeDataTransformer


class DataAcquisitor(ABC):
    """
    Abstraction of
    """

    def __init__(self):
        """
        Definition of Data Acquisitor project
        """
        self.__data_acquisitor_status = None

    @property
    def acquisitor_status(self):
        return self.__data_acquisitor_status

    @acquisitor_status.setter
    def acquisitor_status(self, running_status):
        self.__data_acquisitor_status = running_status

    def get_data_acquisitor_status(self):
        """
        Probing the status of data acquisitor
        :return:
        """
        return self.acquisitor_status()



class WebApiAcquisitor(DataAcquisitor):


    def __init__(self, url: str, data_acq_period=300, connection_retry_times=3, data_transformer=None):
        """
        Concrete class inheritance from `DataAcquisitor`, Extracting Data from public web API
        :param url: The url of public web API to extract data
        :param data_acq_period: The time in seconds of suspending during waining for subsequent data acq event, default: 300 seconds
        :param connection_retry_times: re-try times once the data extraction fails in case of temporary error response(e.g. Too many request)
        :param data_transformer: data parser for transfer raw data extract from we api
        """
        super(WebApiAcquisitor).__init__()

        self.__url = url
        self.__data_acq_period = data_acq_period
        self.__connection_retry_times = connection_retry_times
        self.__data_transformer = data_transformer

        ''' Testing url is available web API '''
        self.__connection_code = requests.get(self.__url).status_code
        if self.__connection_code == 200:
            print('Successfully create WebApiAcquisitor to extract data from {}'.format(self.__url))
        elif self.__connection_code == 429:
            print('Too many request error, try again later')
        else:
            raise requests.ConnectionError('Not valid connection to {} with connection code:{}'.format(self.__url, self.__connection_code))

    def get_connection_code(self) -> int:
        """
        Extract the HTTP request connection code
        :return int: request connection code
        """
        return self.__connection_code

    def stop(self):
        """
        Labeling Data Acquisitor status as stopped, triggering stop the thread target function(run)
        :return:
        """
        print('Calling stop function')
        self.acquisitor_status = 'stopped'

    def run(self):
        self.acquisitor_status = 'running'
        print("Starting Data Acquisitor")

        while self.acquisitor_status == 'running':

            raw_data = self.polling_data().json()
            transfer_data = self.__data_transformer.raw_data_transform_to_df(raw_data)
            print(raw_data)

            time.sleep(self.__data_acq_period)

        print("Data Acquisitor is stopped")

    def polling_data(self):

        data_acq_request = None
        try:
            data_acq_request = requests.get(self.__url)
        except Exception as e:
            e.with_traceback()

        if data_acq_request.status_code == 200:
            return data_acq_request
        elif data_acq_request.status_code == 404:
            print("connection to url get 404 error, please check api url! Stop this data acquisitor")
            self.stop()
        else:
            raise ConnectionError("polling data error, error code: {}, skip this time, waiting for next data acquisition".format(data_acq_request.status_code))


