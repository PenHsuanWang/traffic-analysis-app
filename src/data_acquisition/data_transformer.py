import pandas as pd
import abc


class DataTransformer(abc.ABC):

    def __init__(self):
        pass

    @abc.abstractmethod
    def raw_data_transform_to_df(self, input_raw_data, data_format):
        raise NotImplementedError("DataTransformer is not implement")


class YouBikeDataTransformer(DataTransformer):

    def __init__(self):
        super(YouBikeDataTransformer).__init__()
        pass

    def raw_data_transform_to_df(self, input_raw_data, data_format):

        youbike_data = None
        df_selected = None

        try:
            if data_format == 'taipei':
                youbike_data = input_raw_data.get('retVal').values()
            elif data_format == 'newtaipei':
                youbike_data = input_raw_data

        except Exception as e:
            e.with_traceback()
            raise e

        try:
            df_all = pd.DataFrame(data=youbike_data)
            df_selected = df_all[['sno', 'sna', 'tot', 'sbi', 'sarea', 'mday']]

        except Exception as e:
            e.with_traceback()
            raise e

        finally:
            return df_selected
