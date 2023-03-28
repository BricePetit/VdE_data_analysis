__title__ = "main"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"

import datetime as dt
import multiprocessing
import os
import pandas as pd
import pytz
from typing import NoReturn, List, Optional

from plot_load_curves import (
    plot_average_community,
    average_through_community,
    plot_median_quantile_flukso,
    plot_median_quantile_rtu,
    flukso_plot,
    rtu_plot,
    plot_aggregation,
    plot_data
)
from sms_reaction import find_reaction_report
from utils import (
    check_negative_consumption, resample_dataset, export_to_XLSX
)

# Import constants for the configuration of the execution
from config import (
    TZ,
    NB_SLAVES,
    FLUKSO,
    RTU,
    FMT,
    NEXT_CLOUD,
    # Constants for path
    CURRENT_FOLDER,
    DATASET_FOLDER,
    RESAMPLED_FOLDER,
    PLOT_PATH,
    # Constants for the name of communities
    COMMUNITY_NAME,
    # Constants for the data management
    MANAGE_DATA,
    CONCAT_DATA,
    VERIFY_CONSUMPTION,
    RESAMPLE,
    RESAMPLE_RTU,
    CHECK_DATES,
    # Constants for reactions of messages
    REACTION,
    # Constants for the plotting
    PLOT,
    BASIC_DATA,
    BASIC_PLOT,
    AREA_PLOT,
    PLOT_AVERAGE,
    AVERAGE_COMMUNITY,
    AVERAGE_COMMUNITIES,
    AGGREGATION,
    PLOT_RANGE_RTU,
    PLOT_MEDIAN_QUANTILE_FLUKSO,
    PLOT_MEDIAN_QUANTILE_RTU,
    MEAN_WED_FLUKSO,
    MEAN_WED_RTU,
    # Constants for the autoc onsumption
    AUTO_CONSUMPTION,
    # Variables for CDB
    ALL_CDB,
    ALERTS_CDB,
    MATRIX_ALERTS_CDB,
    SUM_ALERTS_CDB,
    # Variables for ECH
    ALL_ECH,
    ALL_COMMUNAL,
    ALERTS_ECH,
    MATRIX_ALERTS_ECH,
    SUM_ALERTS_ECH,
)


# ----------------------------- #
# ----------MAIN CODE---------- #
# ----------------------------- #
def concat_data(columns_name: List[str], type_concat: str) -> NoReturn:
    """
    Function to concatenate data into one file. The idea is to take each file day by day
    and create a new file month by month or per house.

    :param columns_name:    List of string with the name of each column.
    :param type_concat:     String with the type of the concatenation.
    """
    if columns_name == "":
        print("--------------------Error no columns name--------------------")
    else:
        print("--------------------Processing concatenation...--------------------")
        previous_home: str = ""
        path_to_read: str = f"{NEXT_CLOUD}/download_data"
        home_df: pd.DataFrame = pd.DataFrame(columns=columns_name)
        for file in sorted(os.listdir(path_to_read)):
            if file == '.DS_Store':
                continue
            print(f"----------{file}----------")
            need_save: bool = True
            if previous_home == "":
                previous_home: str = file
            elif (type_concat == "monthly"
                    and
                    (previous_home[:6] != file[:6] or previous_home[12:-7] != file[12:-7])):
                home_df.to_csv(
                    f"{DATASET_FOLDER}/{previous_home[:3]}/{previous_home[:14]}.csv",
                    index=False
                )
                home_df: pd.DataFrame = pd.DataFrame(columns=columns_name)
                need_save: bool = False
            elif type_concat == "yearly" and previous_home[:6] != file[:6]:
                home_df.to_csv(
                    f"{DATASET_FOLDER}/{previous_home[:3]}/{previous_home[:6]}.csv", index=False
                )
                home_df: pd.DataFrame = pd.DataFrame(columns=columns_name)
                need_save: bool = False

            home_df: pd.DataFrame = pd.concat(
                [home_df, pd.read_csv(f"{path_to_read}/{file}")]
            )
            previous_home: str = file
        if need_save and type_concat == "monthly":
            home_df.to_csv(
                f"{DATASET_FOLDER}/{previous_home[:3]}/{previous_home[:14]}.csv", index=False
            )
        elif need_save and type_concat == "yearly":
            home_df.to_csv(
                f"{DATASET_FOLDER}/{previous_home[:3]}/{previous_home[:6]}.csv", index=False
            )


def manage_flukso_data() -> NoReturn:
    """
    Function to manage data.
    """
    # For all communities
    if VERIFY_CONSUMPTION:
        for community in COMMUNITY_NAME:
            print("---------------Managing Data---------------")
            # For all file in the data folder
            # for file in sorted(os.listdir(community)):
            folder_path: str = f"{CURRENT_FOLDER}/{community}"
            for file in sorted(os.listdir(folder_path)):
                if file == '.DS_Store':
                    continue
                filename: str = file[:6]
                print(f"---------------{filename}---------------")
                df: pd.DataFrame = pd.read_csv(f"{folder_path}/{file}")
                # Check if there is a negative consumption
                check_negative_consumption(df, filename)

    # Run the resample function according to Resample boolean value
    if RESAMPLE:
        # Way to parallelize tasks to resample data.
        cdb: List[str] = sorted(os.listdir(f"{DATASET_FOLDER}/CDB"))
        if cdb[0] == '.DS_Store':
            cdb.pop(0)
        ech: List[str] = sorted(os.listdir(f"{DATASET_FOLDER}/ECH"))
        if ech[0] == '.DS_Store':
            ech.pop(0)
        files: List[str] = (cdb + ech)
        # Open a pool of processes to parallelize the resampling.
        with multiprocessing.Pool(NB_SLAVES) as p:
            # Create a dictionary containing the ID of the home and the process.
            resample_map = {
                file[:6]: p.apply_async(
                    resample_dataset,
                    (
                        pd.read_csv(f"{DATASET_FOLDER}/{file[:3]}/{file}"),
                        f"{RESAMPLED_FOLDER}/{file[:3]}",
                        file[:6],
                        {
                            'home_id': 'first',
                            'day': 'first',
                            'p_cons': 'mean',
                            'p_prod': 'mean',
                            'p_tot': 'mean'
                        }
                    )
                )
                for file in files
                if file == f"{file[:6]}.csv"
            }
            # For each process, we wait the end of the execution
            for _, resample in resample_map.items():
                resample.wait()


def manage_rtu_data() -> NoReturn:
    """
    Manage rtu data.
    """
    # Resample rtu data
    if RESAMPLE_RTU:
        df: pd.DataFrame = pd.read_csv(f"{DATASET_FOLDER}/RTU/rtu.csv")
        resample_dataset(
            df, f"{RESAMPLED_FOLDER}/RTU", 'rtu',
            {
                'ip': 'first', 'day': 'first', 'active': 'mean',
                'apparent': 'mean', 'cos_phi': 'mean', 'reactive': 'mean',
                'tension1_2': 'mean', 'tension2_3': 'mean', 'tension3_1': 'mean'
            }
        )


def compute_alert_reaction() -> NoReturn:
    """
    Function for reactions.
    """
    for community in COMMUNITY_NAME:
        print("--------------Computing Alerts--------------")
        path: str = f"{CURRENT_FOLDER}/{community}"
        # For all file in the data folder
        i: int = 0
        for file in sorted(os.listdir(path)):
            if file == '.DS_Store':
                continue
            print(f"---------------{file[:6]}---------------")
            df: pd.DataFrame = pd.read_csv(f"{path}/{file}")
            df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
            df['day']: pd.TimestampSeries = pd.to_datetime(df['day'])
            df: pd.DataFrame = df[df['p_cons'] > 0]
            # Find if a house reacted to the message
            if community == "CDB" and file[:6] in ALL_CDB:
                find_reaction_report(
                    df, ALERTS_CDB, MATRIX_ALERTS_CDB, SUM_ALERTS_CDB, i
                )
                i += 1
            elif community == "ECH" and file[:6] in ALL_ECH:
                find_reaction_report(
                    df, ALERTS_ECH, MATRIX_ALERTS_ECH, SUM_ALERTS_ECH, i
                )
                i += 1
    # Take all home ids and add (%)
    cdb_home_id: List[str] = [f + ' (%)' for f in ALL_CDB]
    ech_home_id: List[str] = [f + ' (%)' for f in ALL_ECH]

    # Export MATRIX_ALERTS_CDB or MATRIX_ALERTS_ECH in excel files
    export_to_XLSX(
        MATRIX_ALERTS_CDB, cdb_home_id, ALERTS_CDB, SUM_ALERTS_CDB, f"{PLOT_PATH}/alerts_cdb.xlsx"
    )
    export_to_XLSX(
        MATRIX_ALERTS_ECH, ech_home_id, ALERTS_ECH, SUM_ALERTS_ECH, f"{PLOT_PATH}/alerts_ech.xlsx"
    )


def plot_average() -> NoReturn:
    """
    Plot average.
    """
    # 2022-08-23
    starting: dt.datetime = dt.datetime(2022, 8, 23, 0, 0, 0).astimezone()
    ending: dt.datetime = dt.datetime(2022, 8, 23, 23, 59, 59).astimezone()
    # Plot an average for a given date for a community
    if AVERAGE_COMMUNITY:
        print("--------------Plotting average--------------")
        for i in [5, 10, 15, 20, 25, 30]:
            plot_average_community(starting, ending, i)

    # Plot the average communities together
    if AVERAGE_COMMUNITIES:
        print("--------------Plotting average through communities--------------")
        for nb_selected_house in [5, 10, 15, 20, 25, 30, 35, 40, 45]:
            print(f"--------------{nb_selected_house} selected houses--------------")
            average_through_community(starting, ending, nb_selected_house)

    # Plot all aggregation
    if AGGREGATION:
        print("--------------Plotting Aggregation--------------")
        plot_aggregation(starting, ending)


def plot_flukso() -> NoReturn:
    """
    Function to plot flukso data.
    """
    cdt: bool = True
    # For all communities
    if BASIC_PLOT or AREA_PLOT:
        for community in COMMUNITY_NAME:
            print("--------------Plotting--------------")
            # For all file in the data folder
            for file in sorted(os.listdir(f"{CURRENT_FOLDER}/{community}")):
                print(f"---------------{file[:6]}---------------")
                df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/{community}/{file}")
                home_id: str = df.at[0, 'home_id']
                starting: dt.datetime = dt.datetime(2022, 5, 17, 0, 0, 0).astimezone()
                ending: dt.datetime = dt.datetime(2022, 5, 17, 23, 59, 59).astimezone()
                path: str = f"{PLOT_PATH}/{community}/{home_id}/{FMT}"
                if not BASIC_DATA:
                    cdt: bool = (
                        int(file[12:14]) == starting.month and int(file[7:11]) == starting.year
                    )
                if cdt and file[:6] not in [
                    'ECHA01', 'ECHASC', 'ECHBUA', 'ECHCOM', 'ECHL09', 'ECHL17'
                ]:
                    # Change it later because we will receive a correct df with the timezone
                    df['ts']: pd.TimestampSeries = (
                        pd.to_datetime(df['ts'])
                        .dt
                        .tz_localize(pytz.timezone('Europe/Brussels'), ambiguous=True)
                    )
                    if BASIC_PLOT:
                        plot_data(
                            df, path, starting, ending,
                            f"Home: {home_id}", 'multiple_flukso',
                            f"{home_id}_{starting}_{ending}_{FMT}"
                        )
                    elif AREA_PLOT:
                        plot_data(
                            df, path, starting, ending,
                            f"Home: {home_id}", 'flukso', f"{home_id}_{starting}_{ending}_{FMT}"
                        )
    plot_average()


def plot_rtu() -> NoReturn:
    """
    Function to plot rtu data.
    """
    if BASIC_DATA:
        file_name: str = 'rtu.csv'
        time_series: pd.Series = (
            pd.date_range("00:00:00", freq='5min', periods=288)
            .to_series()
            .apply(lambda x: x.strftime('%H:%M:%S'))
            .reset_index(drop=True)
        )
    else:
        file_name = 'rtu_15min.csv'
        time_series: pd.Series = (
            pd.date_range("00:00:00", freq='15min', periods=96)
            .to_series()
            .apply(lambda x: x.strftime('%H:%M:%S'))
            .reset_index(drop=True)
        )

    if PLOT_MEDIAN_QUANTILE_RTU:
        print("--------------Plotting RTU quantile--------------")
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/RTU/{file_name}")
        df['ts']: pd.Series = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        plot_median_quantile_rtu(df, f"{PLOT_PATH}/RTU", time_series)

    if PLOT_RANGE_RTU:
        print("--------------Plotting RTU range--------------")
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/RTU/{file_name}")
        starting: dt.datetime = dt.datetime(2022, 12, 21, 0, 0, 0).astimezone()
        ending: dt.datetime = dt.datetime(2022, 12, 21, 23, 59, 59).astimezone()
        plot_data(
            df, f"{PLOT_PATH}/RTU", starting, ending, 'Cabine basse tension',
            'rtu', f"rtu_{starting}_{ending}"
        )

    if MEAN_WED_RTU:
        print("--------------Plotting RTU mean wednesday--------------")
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/RTU/{file_name}")
        df['ts']: pd.Series = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        df: pd.DataFrame = df[(df['ts'].dt.weekday == 2) & (df['ts'].dt.year != 2023)]
        not_during_light: pd.DataFrame = df[~((df['ts'].dt.day == 21) & (df['ts'].dt.month == 12))]
        mean: pd.DataFrame = df.groupby(not_during_light['ts'].dt.time).mean(numeric_only=True)
        rtu_plot(mean, time_series, f"{PLOT_PATH}/RTU", 'Mean wednesday RTU')


def prepare_plot_median_quantile_flukso():
    """
    Function to prepare and apply the plotting of the median and quantiles of flukso data.
    """
    if BASIC_DATA:
        time_series: pd.Series = (
            pd.date_range("00:00:00", freq='8S', periods=10800)
            .to_series()
            .apply(lambda x: x.strftime('%H:%M:%S'))
            .reset_index(drop=True)
        )
    else:
        time_series: pd.Series = (
            pd.date_range("00:00:00", freq='15min', periods=96)
            .to_series()
            .apply(lambda x: x.strftime('%H:%M:%S'))
            .reset_index(drop=True)
        )
    print("----------Ploting quantiles----------")
    print("----------CDB----------")
    for cdb in ALL_CDB:
        print(f"----------{cdb}----------")
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/CDB/{cdb}")
        # Add correct datetime with timezone.
        df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        df: pd.DataFrame = df[df['p_cons'] > 0]
        if df.empty:
            print(f"----------{cdb} is empty----------")
            print("----------There is no cons > 0----------")
            continue
        plot_median_quantile_flukso(
            df, f"{PLOT_PATH}/CDB/{cdb[:6]}", time_series
        )
    print("----------ECH----------")
    for ech in ALL_ECH:
        print(f"----------{ech}----------")
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/ECH/{ech}")
        # Add correct datetime with timezone.
        df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        if df.empty:
            print(f"----------{ech} is empty----------")
            continue
        plot_median_quantile_flukso(
            df, f"{PLOT_PATH}/ECH/{ech[:6]}", time_series
        )


def compute_mean_wednesday_flukso():
    """
    Compute the mean for a typical wednesday in CDB.
    """
    print("----------Compute mean for wednesday----------")
    for cdb in ALL_CDB:
        print(f"----------{cdb}----------")
        time_series: pd.Series = (
            pd.date_range("00:00:00", freq='15min', periods=96)
            .to_series()
            .apply(lambda x: x.strftime('%H:%M:%S'))
            .reset_index(drop=True)
        )
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/CDB/{cdb}")
        df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        df: pd.DataFrame = df[(df['ts'].dt.weekday == 2) & (df['ts'].dt.year != 2023)]
        df: pd.DataFrame = df[df['p_cons'] > 0]
        if df.empty:
            print(f"----------{cdb} is empty----------")
            print("----------There is no cons > 0----------")
            continue
        df: pd.DataFrame = (
            df
            .resample('15min', on='ts')
            .mean(numeric_only=True)
            .reset_index()
        )
        not_during_light: pd.DataFrame = (
            df[~((df['ts'].dt.day == 21) & (df['ts'].dt.month == 12))]
        )
        mean: pd.DataFrame = (
            df
            .groupby(not_during_light['ts'].dt.time)
            .mean(numeric_only=True)
        )
        flukso_plot(
            mean, time_series, f"{PLOT_PATH}/CDB/{cdb[:6]}", f"Mean wednesday {cdb[:6]}"
        )


def all_plots() -> NoReturn:
    """
    Function to plot all data.
    """
    if FLUKSO:
        if PLOT_MEDIAN_QUANTILE_FLUKSO:
            prepare_plot_median_quantile_flukso()

        if PLOT_AVERAGE:
            plot_average()

        if MEAN_WED_FLUKSO:
            compute_mean_wednesday_flukso()
    if RTU:
        plot_rtu()


def compute_auto_consumption(
    df: pd.DataFrame,
    month: int,
    columns: List[str],
    communal_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Function to compute the auto consumption, the total consumption and
    the total production during a month.

    :param df:          DataFrame with data where we need a column 'ts', 'day', 'p_cons', 'p_prod'.
    :param month:       The number of the month (January is 1 and december is 12).
    :param columns:     List with the name of columns.
    :param communal_df:   DataFrame of the common in ECH. If CDB = None.

    :return:            Return a DataFrame with the percentage of auto consumption, the total
                        consumption and the total production.
    """
    months_list: List[str] = [
        'Janvier', 'Février', 'Mars', 'Avril',
        'Mai', 'Juin', 'Juillet', 'Aout',
        'Septembre', 'Octobre', 'Novembre', 'Decembre'
    ]
    # Convert the ts in datetime to manipulate it.
    df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
    # Consider the month that we want to compute
    work_df: pd.DataFrame = df[(df['ts'].dt.month == month)]
    # Check if we are in the case of the ECH or not
    if communal_df is not None:
        communal_df['ts']: pd.TimestampSeries = (
            pd.to_datetime(communal_df['ts'], utc=True)
            .dt
            .tz_convert(TZ)
        )
        communal_df: pd.DataFrame = communal_df[communal_df['ts'].dt.month == month]
        work_df['p_prod']: pd.Series = work_df['p_prod'].add(communal_df['p_prod'], fill_value=0)
        work_df['p_cons']: pd.Series = work_df['p_cons'].add(communal_df['p_cons'], fill_value=0)
    # Keep only periods where the production is negative meaning that there is a production.
    work_df: pd.DataFrame = work_df[work_df["p_prod"] < 0]
    # Keep only periods where the consumption is positive (remove errors).
    work_df: pd.DataFrame = work_df[work_df["p_cons"] > 0]
    # Compute the consumption according to the production. If the production is lower than
    # the consumption we add the production because we need it for the auto consumption and
    # we don't care about the extra consumption. Else, we add the consumption. (Conditional sum)
    p_cons: float = (
        work_df[work_df['p_cons'] <= -work_df['p_prod']]['p_cons'].sum()
        + (-1 * work_df[work_df['p_cons'] > -work_df['p_prod']]['p_prod']).sum()
    )
    # Compute the sum for the production.
    p_prod: float = work_df['p_prod'].sum()
    # Compute the auto consumption.
    percentage_auto_consumption: float = (p_cons / (p_prod if p_prod != 0 else 1)) * 100
    percentage_auto_consumption: float = (
        percentage_auto_consumption * -1
        if percentage_auto_consumption < 0 else percentage_auto_consumption
    )
    # percentage_auto_consumption
    return pd.DataFrame(
        [[months_list[month - 1], percentage_auto_consumption, p_cons, p_prod]],
        columns=columns
    )


def auto_consumption() -> NoReturn:
    """
    Function to apply the auto consumption for CDB and ECH and then,
    we save results in a excel file with the following style.

    CDBXXX      | Auto consumption | total consumption | total production |
    January     |        x         |         x         |         X        |
    February    |        x         |         x         |         X        |
    April       |        x         |         x         |         X        |
    May         |        x         |         x         |         X        |
    July        |        x         |         x         |         X        |
    August      |        x         |         x         |         X        |
    October     |        x         |         x         |         X        |
    December    |        x         |         x         |         X        |
    CDBYYY      | Auto consumption | total consumption | total production |
    January     |        x         |         x         |         X        |
    February    |        x         |         x         |         X        |
    April       |        x         |         x         |         X        |
    May         |        x         |         x         |         X        |
    July        |        x         |         x         |         X        |
    August      |        x         |         x         |         X        |
    October     |        x         |         x         |         X        |
    December    |        x         |         x         |         X        |
    """
    columns: List[str] = ['Month', 'Autoconsommation', 'consommation totale', 'production totale']
    print("--------------------Computing autoconsumption...--------------------")
    res: pd.DataFrame = pd.DataFrame(columns=columns)
    print("--------------------CDB--------------------")
    for house in ALL_CDB:
        print(f"--------------------{house}--------------------")
        df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/CDB/{house}")
        res: pd.DataFrame = pd.concat(
            [
                res,
                pd.DataFrame(
                    [[
                        f"{house}", 'Autoconsommation',
                        'consommation totale', 'production totale'
                    ]],
                    columns=columns
                )
            ]
        )
        for month in [1, 2, 4, 5, 7, 8, 10, 11]:
            res = pd.concat([res, compute_auto_consumption(df, month, columns)])
    print("--------------------Computing of autoconsumption finished !--------------------")
    print("--------------------Saving file...--------------------")
    if res.empty:
        print("--------------------Save aborted! The file is empty--------------------")
    else:
        res.to_excel(excel_writer=f"{PLOT_PATH}/CDB_auto_consumption.xlsx", index=False)
        print("--------------------Save complete !--------------------")
    print("--------------------ECH--------------------")
    res: pd.DataFrame = pd.DataFrame(columns=columns)
    communal_df: pd.DataFrame = pd.DataFrame()
    for communal in ALL_COMMUNAL:
        communal_df = pd.concat(
            [communal_df, pd.read_csv(f"{CURRENT_FOLDER}/ECH/{communal}")]
        )
    if communal_df.empty:
        print("--------------------Computation aborted! No production data--------------------")
    else:
        communal_df: pd.DataFrame = communal_df.groupby('ts').sum(numeric_only=True).reset_index()
        df_echs: pd.DataFrame = pd.DataFrame()
        for house in ALL_ECH:
            print(f"--------------------{house}--------------------")
            df_echs: pd.DataFrame = pd.concat(
                [df_echs, pd.read_csv(f"{CURRENT_FOLDER}/ECH/{communal}")]
            )
        df_echs: pd.DataFrame = df_echs.groupby('ts').sum(numeric_only=True).reset_index()
        for month in [1, 2, 4, 5, 7, 8, 10, 11]:
            res: pd.DataFrame = pd.concat(
                [res, compute_auto_consumption(df_echs, month, columns, communal_df)]
            )
        print("--------------------Computing of autoconsumption finished !--------------------")
        print("--------------------Saving file...--------------------")
        if res.empty:
            print("--------------------Save aborted! The file is empty--------------------")
        else:
            res.to_excel(excel_writer=f"{PLOT_PATH}/ECH_auto_consumption.xlsx", index=False)
            print(res)
            print("--------------------Save complete !--------------------")


def check_empty_date() -> NoReturn:
    """
    Function to info which data are used and which aren't used.
    """
    print("--------------------Creating file with state of data...--------------------")
    for community in COMMUNITY_NAME:
        final_df: pd.DataFrame = pd.DataFrame()
        columns: List = []
        print(f"--------------------{community}--------------------")
        for home in sorted(os.listdir(f"{CURRENT_FOLDER}/{community}")):
            if home == '.DS_Store':
                continue
            print(f"--------------------{home}--------------------")
            df: pd.DataFrame = pd.read_csv(f"{CURRENT_FOLDER}/{community}/{home}")
            df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
            df: pd.DataFrame = df.resample('15min', on='ts').mean(numeric_only=True).reset_index()
            ts: pd.DataFrame = (
                pd.concat(
                    [df['ts'].dt.tz_localize(None), ((df['p_cons'] >= 0) & (df['p_prod'] <= 0))],
                    axis=1
                )
            )
            columns += [home[:6], 'utilisé']
            final_df: pd.DataFrame = pd.concat([final_df, ts], axis=1, ignore_index=True)
        final_df.columns = columns
        final_df.to_excel(
            excel_writer=f"{PLOT_PATH}/{community}_state.xlsx",
            index=False
        )
    print("--------------------Done!--------------------")


def manage_data() -> NoReturn:
    """
    Function to manage data.
    """
    # Manage the data (e.g. resample, etc.)
    if MANAGE_DATA:
        if FLUKSO:
            manage_flukso_data()
        if RTU:
            manage_rtu_data()


def manage_concat() -> NoReturn:
    """
    Function to concat data.
    """
    # Concat small dataset in larger dataset
    columns_name: str = ""
    if FLUKSO:
        columns_name: List[str] = ['home_id', 'day', 'ts', 'p_cons', 'p_prod', 'p_tot']
    elif RTU:
        columns_name: List[str] = [
            'ip', 'day', 'ts', 'active', 'apparent', 'cos_phi', 'reactive',
            'tension1_2', 'tension2_3', 'tension3_1'
        ]
    # yearly or monthly
    concat_data(columns_name, "yearly")


def main() -> NoReturn:
    """
    Main function
    """
    # Concat all data in one file
    if CONCAT_DATA:
        manage_concat()

    # Manage all data
    if MANAGE_DATA:
        manage_data()

    # Compute and show the information about the alert
    if REACTION:
        compute_alert_reaction()

    # Plot
    if PLOT:
        all_plots()

    # Compute the auto consumption
    if AUTO_CONSUMPTION:
        auto_consumption()

    # Check if there is empty date
    if CHECK_DATES:
        check_empty_date()


if __name__ == "__main__":
    main()
