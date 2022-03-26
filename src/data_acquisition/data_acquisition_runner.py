import time
from threading import Thread
from concurrent import futures

from data_acquisitor import WebApiAcquisitor

class DataAcqisutionRunner:

    def __init__(self):

        self._running_data_acq = WebApiAcquisitor(
            url='https://tcgbusfs.blob.core.windows.net/blobyoubike/YouBikeTP.json',
            data_acq_period=1
        )

        self._pool = futures.ThreadPoolExecutor(3)
        self._future = self._pool.submit(self._running_data_acq.run)

        # self._t = Thread(target=self._running_data_acq.run, name='RunningUbikeApiAcq', args=())
        # self._t.daemon = True
        # self._t.start()


    def stop_runner(self):
        # self._running_data_acq.stop()
        self._running_data_acq.stop()
        self._future.cancel()

        print('going to join thread')
        self._pool.shutdown()
        # self._t.join()


if __name__ == '__main__':

    runner = DataAcqisutionRunner()

    time.sleep(3)
    print("here is main thread, going to peer results")
    time.sleep(3)
    print("here is main thread, going to stop results")
    runner.stop_runner()

    print("here is main thread again, finish all the process")

