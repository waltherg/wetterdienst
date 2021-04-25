# -*- coding: utf-8 -*-
# Copyright (c) 2018-2021, earthobservations developers.
# Distributed under the MIT License. See LICENSE for more info.
import json
import logging
import pprint
import sys
from enum import Enum
from pprint import pformat

import click
import cloup
from cloup import ConstraintMixin
from cloup.constraints import If, IsSet, RequireExactly, And, mutually_exclusive
import pandas as pd
from cloup.constraints.conditions import Predicate
from docopt import DocoptExit, docopt
from munch import Munch

from wetterdienst import Kind, Provider, Wetterdienst, __appname__, __version__
from wetterdienst.core.scalar.export import ExportMixin
from wetterdienst.exceptions import ProviderError
from wetterdienst.provider.dwd.forecast import DwdMosmixRequest, DwdMosmixType
from wetterdienst.provider.dwd.observation import (
    DwdObservationDataset,
    DwdObservationPeriod,
    DwdObservationResolution,
)
from wetterdienst.provider.dwd.observation.api import DwdObservationRequest
from wetterdienst.provider.dwd.radar.api import DwdRadarSites
from wetterdienst.provider.eumetnet.opera.sites import OperaRadarSites
from wetterdienst.util.cli import normalize_options, read_list, setup_logging

log = logging.getLogger(__name__)


appname = f"{__appname__} {__version__}"


# def output(thing):
#     for item in thing:
#         if item:
#             if hasattr(item, "value"):
#                 value = item.value
#             else:
#                 value = item
#             print("-", value)

provider_arg = click.argument(
    "provider",
    type=click.Choice(list(map(lambda p: p.name, Provider)), case_sensitive=False),
)

kind_arg = click.argument(
    "kind", type=click.Choice([Kind.OBSERVATION.name, Kind.FORECAST.name], case_sensitive=False)
)
debug_opt = click.option("--debug/--no-debug", default=False)


def setup_logging(debug: bool):
    # Setup logging.
    log_level = logging.INFO

    if debug:  # pragma: no cover
        log_level = logging.DEBUG

    log.setLevel(log_level)


def get_api(provider: str, kind: str):
    try:
        api = Wetterdienst(provider, kind)
    except ProviderError as e:
        click.Abort(e.str)

    print(api)

    return api


@cloup.group()
def wetterdienst():
    pass


@wetterdienst.command("restapi")
@cloup.option("--listen", type=click.STRING, default=None)
@cloup.option("--reload", type=click.BOOL, default=False)
@debug_opt
def restapi(listen: str, reload: bool, debug: bool):
    setup_logging(debug)

    # Run HTTP service.
    log.info(f"Starting {appname}")
    log.info(f"Starting HTTP web service on http://{listen}")

    from wetterdienst.ui.restapi import start_service

    start_service(listen, reload=reload)

    return


@wetterdienst.command("explorer")
@cloup.option("--listen", type=click.STRING, default=None)
@cloup.option("--reload", type=click.BOOL, default=False)
@debug_opt
def explorer(listen: str, reload: bool, debug: bool):
    setup_logging(debug)

    log.info(f"Starting {appname}")
    log.info(f"Starting Explorer web service on http://{listen}")
    from wetterdienst.ui.explorer.app import start_service

    start_service(listen, reload=reload)
    return


@wetterdienst.group()
def about():
    pass


@about.command()
@provider_arg
@kind_arg
# @click.option("--dataset", type=click.STRING, default=False),
@cloup.option("--filter", type=click.STRING, default=False)
@debug_opt
def coverage(provider, kind, filter_, debug):
    setup_logging(debug)

    api = get_api(provider=provider, kind=kind)

    # dataset = kwargs.get("dataset")
    # if dataset:
    #     dataset = read_list(dataset)

    meta = api.discover(
        filter_=filter_,
        # dataset=dataset,
        flatten=False,
    )

    print(meta)

    return


@about.command()
@provider_arg
@kind_arg
@cloup.option_group(
    "(DWD only) information from PDF documents",
    click.option("--dataset", type=click.STRING),
    click.option("--resolution", type=click.STRING),
    click.option("--period", type=click.STRING),
    click.option("--language", type=click.Choice(["en", "de"], case_sensitive=False)),
    constraint=cloup.constraints.require_all,
)
@debug_opt
def fields(provider, kind, dataset, resolution, period, language, **kwargs):
    api = get_api(provider, kind)

    if api.provider != Provider.DWD and kwargs.get("fields"):
        raise click.BadParameter("'fields' command only available for provider 'DWD'")

    metadata = api.describe_fields(
        dataset=read_list(dataset),  # read_list(kwargs.get("dataset")),
        resolution=resolution,
        period=read_list(period),
        language=language,
    )
    output = pformat(dict(metadata))
    print(output)
    # elif API.provider == Provider.DWD and fields:
    #     metadata = DwdObservationRequest.describe_fields(
    #         dataset=read_list(options.parameter),
    #         resolution=options.resolution,
    #         period=read_list(options.period),
    #         language=options.language,
    #     )
    #     output = pformat(dict(metadata))
    #     print(output)
    return


@wetterdienst.command()
@provider_arg
@kind_arg
@cloup.option_group(
    "Station id filtering",
    click.option("--station_id", type=click.STRING),
    constraint=cloup.constraints.all_or_none
)
@cloup.option_group(
    "Station name filtering",
    click.option("--name", type=click.STRING),
    constraint=cloup.constraints.all_or_none
)
@cloup.option_group(
    "Latitude-Longitude rank filtering",
    cloup.option("--latitude", type=click.FLOAT),
    cloup.option("--longitude", type=click.FLOAT),
    cloup.option("--rank", type=click.INT),
    cloup.option("--distance", type=click.FLOAT),
    constraint=If(IsSet("latitude") & IsSet("longitude"), then=RequireExactly(3), else_=cloup.constraints.accept_none)
)
# @cloup.option("--latitude", type=click.FLOAT)
# @cloup.option("--longitude", type=click.FLOAT)
# @cloup.option("--rank", type=click.INT)
# @cloup.option("--distance", type=click.FLOAT)
# @cloup.constraint(
#     If(
#         IsSet("latitude"), #  & IsSet("longitude"),
#         then=RequireExactly(3),
#         else_=cloup.constraints.accept_none),
#     ["latitude", "longitude", "rank", "distance"]
# )
# @cloup.option_group(
#     "Latitude-Longitude distance filtering",
#     cloup.option("--latitude", type=click.FLOAT),
#     cloup.option("--longitude", type=click.FLOAT),
#     cloup.option("--distance", type=click.FLOAT),
#     constraint=cloup.constraints.all_or_none
# )
@cloup.option_group(
    "BBOX filtering",
    click.option("--left", type=click.FLOAT),
    click.option("--bottom", type=click.FLOAT),
    click.option("--right", type=click.FLOAT),
    click.option("--top", type=click.FLOAT),
    constraint=cloup.constraints.all_or_none
)
@cloup.option_group(
    "SQL filtering",
    click.option("--sql", type=click.STRING),
    constraint=cloup.constraints.all_or_none
)
@cloup.option("--format", default="json")
@debug_opt
def stations(provider, kind, **kwargs):
    setup_logging(kwargs.get("debug"))

    api = get_api(provider=provider, kind=kind)

    if kwargs.get("station_id"):
        station_id = kwargs.get("station_id")
        station_id = station_id.split(",")

        request = api.filter_by_station_id(station_id)
    elif kwargs.get("name"):
        name = kwargs.get("name")
        name = name.split(",")

        request = api.filter_by_name(name)
    elif kwargs.get("latitude"):
        If(
            IsSet("latitude") & IsSet("longitude"),
            then=RequireExactly(3),
            else_=cloup.constraints.accept_none)

        if kwargs.get("rank"):
            request = api.filter_by_rank(
                latitude=float(kwargs.get("latitude")),
                longitude=float(kwargs.get("longitude")),
                rank=int(kwargs.get("rank")),
            )
        elif kwargs.get("distance"):
            request = api.filter_by_distance(
                latitude=float(kwargs.get("latitude")),
                longitude=float(kwargs.get("longitude")),
                distance=int(kwargs.get("distance")),
            )
    elif kwargs.get("left"):
        request = stations.filter_by_bbox(
            left=kwargs.get("left"),
            bottom=kwargs.get("bottom"),
            right=kwargs.get("right"),
            top=kwargs.get("top"),
        )

    df = request.df

    if kwargs.get("sql"):
        # TODO: make own function in request class providing sql filtering
        request = stations.all()

        df = ExportMixin._filter_by_sql(request.df, kwargs.get("sql"))

    if kwargs.get("target"):
        ExportMixin.to_target(kwargs.get("target"))



    return


# @app.command("radar")
def radar(
    dwd: bool = False,
    odim_code: str = False,
    wmo_code: str = False,
    country_name: str = False,
):
    if dwd:
        data = DwdRadarSites().all()
    else:
        if odim_code:
            data = OperaRadarSites().by_odimcode(odim_code)
        elif wmo_code:
            data = OperaRadarSites().by_wmocode(wmo_code)
        elif country_name:
            data = OperaRadarSites().by_countryname(country_name)
        else:
            data = OperaRadarSites().all()

    output = json.dumps(data, indent=4)
    print(output)
    return


def run():
    """
    Usage:
      wetterdienst dwd observation stations --parameter=<parameter> --resolution=<resolution> --period=<period> [--station=<station>] [--latitude=<latitude>] [--longitude=<longitude>] [--rank=<rank>] [--distance=<distance>] [--sql=<sql>] [--format=<format>] [--target=<target>]
      wetterdienst dwd observation values --parameter=<parameter> --resolution=<resolution> [--station=<station>] [--period=<period>] [--date=<date>] [--tidy] [--sql=<sql>] [--format=<format>] [--target=<target>]
      wetterdienst dwd observation values --parameter=<parameter> --resolution=<resolution> --latitude=<latitude> --longitude=<longitude> [--period=<period>] [--rank=<rank>] [--distance=<distance>] [--tidy] [--date=<date>] [--sql=<sql>] [--format=<format>] [--target=<target>]
      wetterdienst dwd forecast stations [--parameter=<parameter>] [--mosmix-type=<mosmix-type>] [--date=<date>] [--station=<station>] [--latitude=<latitude>] [--longitude=<longitude>] [--rank=<rank>] [--distance=<distance>] [--sql=<sql>] [--format=<format>] [--target=<target>]
      wetterdienst dwd forecast values --parameter=<parameter> [--mosmix-type=<mosmix-type>] --station=<station> [--date=<date>] [--tidy] [--sql=<sql>] [--format=<format>] [--target=<target>]
      wetterdienst dwd about [parameters] [resolutions] [periods]
      wetterdienst dwd about coverage [--parameter=<parameter>] [--resolution=<resolution>] [--period=<period>]
      wetterdienst dwd about fields --parameter=<parameter> --resolution=<resolution> --period=<period> [--language=<language>]
      wetterdienst radar stations [--odim-code=<odim-code>] [--wmo-code=<wmo-code>] [--country-name=<country-name>]
      wetterdienst dwd radar stations
      wetterdienst restapi [--listen=<listen>] [--reload]
      wetterdienst explorer [--listen=<listen>] [--reload]
      wetterdienst --version
      wetterdienst (-h | --help)

    Options:
      --parameter=<parameter>       Parameter Set/Parameter, e.g. "kl" or "precipitation_height", etc.
      --resolution=<resolution>     Dataset resolution: "annual", "monthly", "daily", "hourly", "minute_10", "minute_1"
      --period=<period>             Dataset period: "historical", "recent", "now"
      --station=<station>           Comma-separated list of station identifiers
      --latitude=<latitude>         Latitude for filtering by geoposition.
      --longitude=<longitude>       Longitude for filtering by geoposition.
      --rank=<rank>                 Rank of nearby stations when filtering by geoposition.
      --distance=<distance>         Maximum distance in km when filtering by geoposition.
      --date=<date>                 Date for filtering data. Can be either a single date(time) or
                                    an ISO-8601 time interval, see https://en.wikipedia.org/wiki/ISO_8601#Time_intervals.
      --mosmix-type=<mosmix-type>   type of mosmix, either 'small' or 'large'
      --sql=<sql>                   SQL query to apply to DataFrame.
      --format=<format>             Output format. [Default: json]
      --target=<target>             Output target for storing data into different data sinks.
      --language=<language>         Output language. [Default: en]
      --version                     Show version information
      --debug                       Enable debug messages
      --listen=<listen>             HTTP server listen address.
      --reload                      Run service and dynamically reload changed files
      -h --help                     Show this screen


    Examples requesting observation stations:

      # Get list of all stations for daily climate summary data in JSON format
      wetterdienst dwd observation stations --parameter=kl --resolution=daily --period=recent

      # Get list of all stations in CSV format
      wetterdienst dwd observation stations --parameter=kl --resolution=daily --period=recent --format=csv

      # Get list of specific stations
      wetterdienst dwd observation stations --resolution=daily --parameter=kl --period=recent --station=1,1048,4411

      # Get list of specific stations in GeoJSON format
      wetterdienst dwd observation stations --resolution=daily --parameter=kl --period=recent --station=1,1048,4411 --format=geojson

    Examples requesting observation values:

      # Get daily climate summary data for specific stations
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent

      # Get daily climate summary data for specific stations in CSV format
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent

      # Get daily climate summary data for specific stations in tidy format
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent --tidy

      # Limit output to specific date
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent --date=2020-05-01

      # Limit output to specified date range in ISO-8601 time interval format
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent --date=2020-05-01/2020-05-05

      # The real power horse: Acquire data across historical+recent data sets
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --date=1969-01-01/2020-06-11

      # Acquire monthly data for 2020-05
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=monthly --date=2020-05

      # Acquire monthly data from 2017-01 to 2019-12
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=monthly --date=2017-01/2019-12

      # Acquire annual data for 2019
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=annual --date=2019

      # Acquire annual data from 2010 to 2020
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=annual --date=2010/2020

      # Acquire hourly data
      wetterdienst dwd observation values --station=1048,4411 --parameter=air_temperature --resolution=hourly --period=recent --date=2020-06-15T12

    Examples requesting forecast stations:

      wetterdienst dwd forecast stations

    Examples requesting forecast values:

      wetterdienst dwd forecast values --parameter=ttt,ff --station=65510

    Examples using geospatial features:

      # Acquire stations and readings by geoposition, request specific number of nearby stations.
      wetterdienst dwd observation stations --resolution=daily --parameter=kl --period=recent --lat=49.9195 --lon=8.9671 --rank=5
      wetterdienst dwd observation values --resolution=daily --parameter=kl --period=recent --lat=49.9195 --lon=8.9671 --rank=5 --date=2020-06-30

      # Acquire stations and readings by geoposition, request stations within specific distance.
      wetterdienst dwd observation stations --resolution=daily --parameter=kl --period=recent --lat=49.9195 --lon=8.9671 --distance=25
      wetterdienst dwd observation values --resolution=daily --parameter=kl --period=recent --lat=49.9195 --lon=8.9671 --distance=25 --date=2020-06-30

    Examples using SQL filtering:

      # Find stations by state.
      wetterdienst dwd observation stations --parameter=kl --resolution=daily --period=recent --sql="SELECT * FROM data WHERE state='Sachsen'"

      # Find stations by name (LIKE query).
      wetterdienst dwd observation stations --parameter=kl --resolution=daily --period=recent --sql="SELECT * FROM data WHERE lower(station_name) LIKE lower('%dresden%')"

      # Find stations by name (regexp query).
      wetterdienst dwd observation stations --parameter=kl --resolution=daily --period=recent --sql="SELECT * FROM data WHERE regexp_matches(lower(station_name), lower('.*dresden.*'))"

      # Filter measurements: Display daily climate observation readings where the maximum temperature is below two degrees celsius.
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent --sql="SELECT * FROM data WHERE temperature_air_max_200 < 2.0;"

      # Filter measurements: Same as above, but use tidy format.
      # FIXME: Currently, this does not work, see https://github.com/earthobservations/wetterdienst/issues/377.
      wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent --sql="SELECT * FROM data WHERE parameter='temperature_air_max_200' AND value < 2.0;" --tidy

    Examples for inquiring metadata:

      # Display list of available parameters (air_temperature, precipitation, pressure, ...)
      wetterdienst dwd about parameters

      # Display list of available resolutions (10_minutes, hourly, daily, ...)
      wetterdienst dwd about resolutions

      # Display list of available periods (historical, recent, now)
      wetterdienst dwd about periods

      # Display coverage/correlation between parameters, resolutions and periods.
      # This can answer questions like ...
      wetterdienst dwd about coverage

      # Tell me all periods and resolutions available for 'air_temperature'.
      wetterdienst dwd about coverage --parameter=air_temperature

      # Tell me all parameters available for 'daily' resolution.
      wetterdienst dwd about coverage --resolution=daily

    Examples for exporting data to files:

      # Export list of stations into spreadsheet
      wetterdienst dwd observations stations --parameter=kl --resolution=daily --period=recent --target=file://stations.xlsx

      # Shortcut command for fetching readings
      alias fetch="wetterdienst dwd observations values --station=1048,4411 --parameter=kl --resolution=daily --period=recent"

      # Export readings into spreadsheet (Excel-compatible)
      fetch --target="file://observations.xlsx"

      # Export readings into Parquet format and display head of Parquet file
      fetch --target="file://observations.parquet"

      # Check Parquet file
      parquet-tools schema observations.parquet
      parquet-tools head observations.parquet

      # Export readings into Zarr format
      fetch --target="file://observations.zarr"

    Examples for exporting data to databases:

      # Shortcut command for fetching readings
      alias fetch="wetterdienst dwd observation values --station=1048,4411 --parameter=kl --resolution=daily --period=recent"

      # Store readings to DuckDB
      fetch --target="duckdb:///dwd.duckdb?table=weather"

      # Store readings to InfluxDB
      fetch --target="influxdb://localhost/?database=dwd&table=weather"

      # Store readings to CrateDB
      fetch --target="crate://localhost/?database=dwd&table=weather"

    Invoke the HTTP REST API service:

      # Start service on standard port, listening on http://localhost:7890.
      wetterdienst restapi

      # Start service on standard port and watch filesystem changes.
      # This is suitable for development.
      wetterdienst restapi --reload

      # Start service on public interface and specific port.
      wetterdienst restapi --listen=0.0.0.0:8890

    Invoke the Wetterdienst Explorer UI service:

      # Start service on standard port, listening on http://localhost:7891.
      wetterdienst explorer

      # Start service on standard port and watch filesystem changes.
      # This is suitable for development.
      wetterdienst explorer --reload

      # Start service on public interface and specific port.
      wetterdienst explorer --listen=0.0.0.0:8891

    """
    appname = f"{__appname__} {__version__}"

    # Read command line options.
    options = normalize_options(docopt(run.__doc__, version=appname))

    # Setup logging.
    debug = options.get("debug")

    log_level = logging.INFO

    if debug:  # pragma: no cover
        log_level = logging.DEBUG

    setup_logging(log_level)

    # Run HTTP service.
    if options.restapi:  # pragma: no cover
        listen_address = options.listen
        log.info(f"Starting {appname}")
        log.info(f"Starting HTTP web service on http://{listen_address}")
        from wetterdienst.ui.restapi import start_service

        start_service(listen_address, reload=options.reload)
        return

    # Run UI service.
    if options.explorer:  # pragma: no cover
        listen_address = options.listen
        log.info(f"Starting {appname}")
        log.info(f"Starting UI web service on http://{listen_address}")
        from wetterdienst.ui.explorer.app import start_service

        start_service(listen_address, reload=options.reload)
        return

    # Handle radar data inquiry. Currently, "stations only".
    if options.radar:
        if options.dwd:
            data = DwdRadarSites().all()
        else:
            if options.odim_code:
                data = OperaRadarSites().by_odimcode(options.odim_code)
            elif options.wmo_code:
                data = OperaRadarSites().by_wmocode(options.wmo_code)
            elif options.country_name:
                data = OperaRadarSites().by_countryname(options.country_name)
            else:
                data = OperaRadarSites().all()

        output = json.dumps(data, indent=4)
        print(output)
        return

    # Output domain information.
    if options.about:
        about(options)
        return

    # Sanity checks.
    if (options["values"] or options.forecast) and options.format == "geojson":
        raise KeyError("GeoJSON format only available for stations output")

    # Acquire station list, also used for readings if required.
    # Filtering applied for distance (a.k.a. nearby) and pre-selected stations
    stations = None
    if options.observation:
        stations = DwdObservationRequest(
            parameter=read_list(options.parameter),
            resolution=options.resolution,
            period=options.period,
            tidy=options.tidy,
            si_units=False,
        )
    elif options.forecast:
        stations = DwdMosmixRequest(
            parameter=read_list(options.parameter),
            mosmix_type=DwdMosmixType.LARGE,
            tidy=options.tidy,
            si_units=False,
        )

    if options.latitude and options.longitude:
        if options.rank:
            stations = stations.filter_by_rank(
                latitude=float(options.latitude),
                longitude=float(options.longitude),
                rank=int(options.rank),
            )
        elif options.distance:
            stations = stations.filter_by_distance(
                latitude=float(options.latitude),
                longitude=float(options.longitude),
                distance=int(options.distance),
            )
        else:
            raise DocoptExit("Geospatial queries need either --rank or --distance")
        results = stations

    elif options.station:
        results = stations.filter_by_station_id(read_list(options.station))

    else:
        results = stations.all()

    df = pd.DataFrame()

    if options.stations:
        pass

    elif options["values"]:
        try:
            # TODO: Add stream-based processing here.
            results = results.values.all()
        except ValueError as ex:
            log.exception(ex)
            sys.exit(1)

    df = results.df

    if df.empty:
        log.error("No data available for given constraints")
        sys.exit(1)

    # Filter readings by datetime expression.
    if options["values"] and options.date:
        results.filter_by_date(options.date)

    # Apply filtering by SQL.
    if options.sql:
        if options.tidy:
            log.error("Combining SQL filtering with tidy format not possible")
            sys.exit(1)

        log.info(f"Filtering with SQL: {options.sql}")
        results.filter_by_sql(options.sql)

    # Emit to data sink, e.g. write to database.
    if options.target:
        results.to_target(options.target)
        return

    # Render to output format.
    try:
        if options.format == "json":
            output = results.to_json()
        elif options.format == "csv":
            output = results.to_csv()
        elif options.format == "geojson":
            output = results.to_geojson()
        else:
            raise KeyError("Unknown output format")

    except KeyError as ex:
        log.error(f'{ex}. Output format must be one of "json", "geojson", "csv".')
        sys.exit(1)

    print(output)


def about(options: Munch):
    """
    Output possible arguments for command line options
    "--parameter", "--resolution" and "--period".

    :param options: Normalized docopt command line options.
    """

    def output(thing):
        for item in thing:
            if item:
                if hasattr(item, "value"):
                    value = item.value
                else:
                    value = item
                print("-", value)

    if options.parameters:
        output(DwdObservationDataset)

    elif options.resolutions:
        output(DwdObservationResolution)

    elif options.periods:
        output(DwdObservationPeriod)

    elif options.coverage:
        metadata = DwdObservationRequest.discover(
            filter_=options.resolution,
            dataset=read_list(options.parameter),
            flatten=False,
        )
        output = json.dumps(metadata, indent=4)
        print(output)

    elif options.fields:
        metadata = DwdObservationRequest.describe_fields(
            dataset=read_list(options.parameter),
            resolution=options.resolution,
            period=read_list(options.period),
            language=options.language,
        )
        output = pformat(dict(metadata))
        print(output)

    else:
        log.error(
            'Please invoke "wetterdienst dwd about" with one of these subcommands:'
        )
        output(["parameters", "resolutions", "periods", "coverage"])
        sys.exit(1)


if __name__ == "__main__":
    wetterdienst()
