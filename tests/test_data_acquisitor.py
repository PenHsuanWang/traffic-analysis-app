def test_get_connection_code():
    from src.data_acquisition.data_acquisitor import WebApiAcquisitor
    from src.data_acquisition.data_transformer import YouBikeDataTransformer

    assert True


def test_polling_data():
    from src.data_acquisition.data_acquisitor import WebApiAcquisitor
    from src.data_acquisition.data_transformer import YouBikeDataTransformer

    web_api_data_acquisitor = WebApiAcquisitor(
        url='https://tcgbusfs.blob.core.windows.net/blobyoubike/YouBikeTP.json',
        data_acq_period=30,
        connection_retry_times=3,
        data_transformer=YouBikeDataTransformer()
    )

    try:
        raw_data = web_api_data_acquisitor.polling_data().json()
        print(raw_data)
        assert True
    except Exception as e:
        e.with_traceback()
        assert False

def test_url_inspector():
    import requests

    data_acq_request = requests.get('https://tcgbusfs.blob.core.windows.net/blobyoubike/YouBikeTP.json')
    print(data_acq_request.status_code)
    if data_acq_request.status_code == 200:
        print(data_acq_request.json())
        assert True
    else:
        assert False
