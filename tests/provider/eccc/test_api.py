# -*- coding: utf-8 -*-
# Copyright (c) 2018-2021, earthobservations developers.
# Distributed under the MIT License. See LICENSE for more info.
import numpy as np
import pandas as pd
import pytz
from pandas._testing import assert_frame_equal

from wetterdienst.provider.eccc.observation import EcccObservationRequest


def test_eccc_api_stations():
    request = EcccObservationRequest(
        parameter="DAILY",
        resolution="DAILY",
        start_date="1990-01-01",
        end_date="1990-01-02",
        humanize=True,
        tidy=True,
        si_units=False,
    ).filter_by_station_id(station_id=(14,))

    expected = pd.DataFrame(
        {
            "station_id": ["14"],
            "from_date": [pd.Timestamp("1984-01-01", tz=pytz.UTC)],
            "to_date": [pd.Timestamp("1996-01-01", tz=pytz.UTC)],
            "height": [4.0],
            "latitude": [48.87],
            "longitude": [-123.28],
            "name": ["ACTIVE PASS"],
            "state": ["BRITISH COLUMBIA"],
        }
    )

    assert_frame_equal(request.df, expected)


def test_eccc_api_values():
    request = EcccObservationRequest(
        parameter="DAILY",
        resolution="DAILY",
        start_date="1980-01-01",
        end_date="1980-01-02",
        humanize=True,
        tidy=True,
        si_units=False,
    ).filter_by_station_id(station_id=(1652,))

    values = request.values.all().df

    expected_df = pd.DataFrame(
        {
            "date": [
                pd.Timestamp("1980-01-01", tz=pytz.UTC),
                pd.Timestamp("1980-01-02", tz=pytz.UTC),
            ]
            * 11,
            "parameter": pd.Categorical(
                [
                    "temperature_air_max_200",
                    "temperature_air_max_200",
                    "temperature_air_min_200",
                    "temperature_air_min_200",
                    "temperature_air_200",
                    "temperature_air_200",
                    "heating_degree_days",
                    "heating_degree_days",
                    "cooling_degree_days",
                    "cooling_degree_days",
                    "precipitation_height_rain",
                    "precipitation_height_rain",
                    "snow_depth_new",
                    "snow_depth_new",
                    "precipitation_height",
                    "precipitation_height",
                    "snow_depth",
                    "snow_depth",
                    "wind_direction_max_velocity",
                    "wind_direction_max_velocity",
                    "wind_gust_max",
                    "wind_gust_max",
                ]
            ),
            "value": [
                -16.3,
                -16.4,
                -29.1,
                -28.3,
                -22.7,
                -22.4,
                40.7,
                40.4,
                0.0,
                0.0,
                0.0,
                0.0,
                1.8,
                0.0,
                0.8,
                0.0,
                19.0,
                20.0,
                np.NaN,
                np.NaN,
                np.NaN,
                np.NaN,
            ],
            "quality": pd.Categorical([np.NaN] * 22),
            "station_id": pd.Categorical(["1652"] * 22),
            "dataset": pd.Categorical(["daily"] * 22),
        }
    )

    assert_frame_equal(
        values.reset_index(drop=True), expected_df, check_categorical=False
    )
