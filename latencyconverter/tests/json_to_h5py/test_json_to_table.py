from latencyconverter.utilities import json_to_hdf5


def test_json_to_table():
    latency_dict = {
        'availability': [
            {
                "id": "QW.QWCC01.HNN",
                "intervals": [
                    {
                        "startTime": "2022-02-13T00:00:00.000000000Z",
                        "endTime": "2022-02-13T00:00:00.999988426Z",
                        "actualNanosecondsAvailable": 189988426,
                        "expectedNanosecondsAvailable": 999988426,
                        "percentAvailability": 18.999062495149317,
                        "latency": {
                            "minimum": 2.408992,
                            "average": 2.408992,
                            "maximum": 2.408992
                        },
                        "retx": {
                            "retxPackets": 0,
                            "allPackets": 1,
                            "retxPercent": 0.0
                        }
                    }
                ]
            }
        ]
    }

    test_df = json_to_hdf5.json_to_table(latency_dict)
    assert 'QW.QWCC01.HNN' in list(test_df['channel'])
    assert 2.408992 in list(test_df['latency'])
