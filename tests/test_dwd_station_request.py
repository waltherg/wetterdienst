import pytest
from python_dwd.dwd_station_request import DWDStationRequest
from python_dwd.exceptions.start_date_end_date_exception import StartDateEndDateError
from python_dwd.enumerations.parameter_enumeration import Parameter
from python_dwd.enumerations.period_type_enumeration import PeriodType
from python_dwd.enumerations.time_resolution_enumeration import TimeResolution


def test_parse_station_id_to_list_of_integers():
    # @todo
    assert True


def test_dwd_station_request():
    assert DWDStationRequest(station_ids=[1], time_resolution="daily", parameter="kl", period_type="historical") \
           == [[1], Parameter.CLIMATE_SUMMARY, TimeResolution.DAILY, [PeriodType.HISTORICAL], None, None]

    assert DWDStationRequest(station_ids=[1],
                             parameter=Parameter.CLIMATE_SUMMARY,
                             period_type=PeriodType.HISTORICAL,
                             time_resolution=TimeResolution.DAILY) == \
           [[1], Parameter.CLIMATE_SUMMARY, TimeResolution.DAILY, [PeriodType.HISTORICAL], None, None]


def test_station_id():
    with pytest.raises(ValueError):
        DWDStationRequest(station_ids="test",
                          parameter=Parameter.CLIMATE_SUMMARY,
                          period_type=PeriodType.HISTORICAL,
                          time_resolution=TimeResolution.DAILY)


def test_parameter_enumerations():
    with pytest.raises(ValueError):
        DWDStationRequest(station_ids=[1],
                          parameter="kl",
                          period_type="now",
                          time_resolution="daily")


def test_time_input():
    with pytest.raises(ValueError):
        DWDStationRequest(station_ids=[1],
                          parameter=Parameter.CLIMATE_SUMMARY,
                          time_resolution=TimeResolution.DAILY,
                          start_date="1971-01-01")

        with pytest.raises(ValueError):
            DWDStationRequest(station_ids=[1],
                              parameter=Parameter.CLIMATE_SUMMARY,
                              time_resolution=TimeResolution.DAILY,
                              period_type=PeriodType.HISTORICAL,
                              start_date="1971-01-01")

    with pytest.raises(StartDateEndDateError):
        DWDStationRequest(station_ids=[1],
                          parameter=Parameter.CLIMATE_SUMMARY,
                          time_resolution=TimeResolution.DAILY,
                          start_date="1971-01-01",
                          end_date="1951-01-01")
