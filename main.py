__title__ = "main"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"

import datetime as dt
import multiprocessing
import os
import pandas as pd
import pytz
from typing import NoReturn, List

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
    # Constants for path
    DATASET_FOLDER,
    RESAMPLED_FOLDER,
    # Constants for the name of communities
    COMMUNITY_NAME,
    # Constants for the data management
    MANAGE_DATA,
    CONCAT_DATA,
    VERIFY_CONSUMPTION,
    RESAMPLE,
    RESAMPLE_RTU,
    # Constants for reactions of messages
    REACTION,
    # Constants for the plotting
    PLOT,
    SEC8,
    BASIC_PLOT,
    AREA_PLOT,
    AVERAGE_COMMUNITY,
    AVERAGE_COMMUNITIES,
    AGGREGATION,
    PLOT_RANGE_RTU,
    PLOT_MEDIAN_QUANTILE_FLUKSO,
    PLOT_MEDIAN_QUANTILE_RTU,
    MEAN_WED_FLUKSO,
    MEAN_WED_RTU,
    # Variables for CDB
    REPORT_CDB,
    ALL_CDB,
    ALERTS_CDB,
    MATRIX_ALERTS_CDB,
    SUM_ALERTS_CDB,
    # Variables for ECH
    REPORT_ECH,
    ALL_ECH,
    ALERTS_ECH,
    MATRIX_ALERTS_ECH,
    SUM_ALERTS_ECH,
)


# ----------------------------- #
# ----------MAIN CODE---------- #
# ----------------------------- #


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
            folder_path: str = DATASET_FOLDER + '/' + community + '_REPORT'
            files = sorted(os.listdir(folder_path))
            for file in sorted(files):
                filename = file[:6]
                print("---------------" + filename + "---------------")
                # df = pd.read_csv(community + '/' + file)
                df = pd.read_csv(folder_path + '/' + file)

                # Check if there is a negative consumption
                check_negative_consumption(df, filename)

    # Run the resample function according to Resample boolean value
    if RESAMPLE:
        # Way to parallelize tasks to resample data.
        files = (
            sorted(os.listdir(DATASET_FOLDER + '/CDB_REPORT'))
            + sorted(os.listdir(DATASET_FOLDER + '/ECH_REPORT'))
        )
        # Open a pool of processes to parallelize the resampling.
        with multiprocessing.Pool(NB_SLAVES) as p:
            # Create a dictionary containing the ID of the home and the process.
            resample_map = {
                file[:6]: p.apply_async(
                    resample_dataset,
                    (
                        pd.read_csv(f"{DATASET_FOLDER}/{file[:3]}_REPORT/{file}"),
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
        df = pd.read_csv('dataset/RTU/rtu.csv')
        resample_dataset(
            df, RESAMPLED_FOLDER + '/RTU', 'rtu',
            {
                'ip': 'first', 'day': 'first', 'active': 'mean',
                'apparent': 'mean', 'cos_phi': 'mean', 'reactive': 'mean',
                'tension1_2': 'mean', 'tension2_3': 'mean', 'tension3_1': 'mean'
            }
        )


def concat_data(columns_name: List[str], type_concat: str) -> NoReturn:
    """
    Function to concatenate data into one file. The idea is to take each file day by day
    and create a new file month by month.

    :param columns_name:    List of string with the name of each column.
    :param type_concat:     String with the type of the concatenation.
    """
    if columns_name == "":
        print('Error no columns name')
    else:
        print("Processing concatenation...")
        for community in COMMUNITY_NAME:
            data_path: str = f"{DATASET_FOLDER}/{community}"
            previous_home: str = ""
            home_df: pd.DataFrame = pd.DataFrame(columns=columns_name)
            files_list = sorted(os.listdir(community))
            if files_list[0] == '.DS_Store':
                files_list.pop(0)
            for file in files_list:
                print(f"----------{file[:6]}----------")
                need_save: bool = True
                if previous_home == "":
                    previous_home = file
                elif (type_concat == "monthly"
                        and
                        (previous_home[:6] != file[:6] or previous_home[12:-7] != file[12:-7])):
                    home_df.to_csv(
                        f"{data_path}_REPORT/{previous_home[:14]}.csv",
                        index=False
                    )
                    home_df: pd.DataFrame = pd.DataFrame(columns=columns_name)
                    need_save: bool = False
                elif type_concat == "yearly" and previous_home[:6] != file[:6]:
                    home_df.to_csv(
                        f"{data_path}_REPORT/{previous_home[:6]}.csv", index=False
                    )
                    home_df: pd.DataFrame = pd.DataFrame(columns=columns_name)
                    need_save: bool = False

                home_df = pd.concat([home_df, pd.read_csv(data_path + '/' + file)])
                previous_home = file
            if need_save and type_concat == "monthly":
                home_df.to_csv(
                    f"{data_path}_REPORT/{previous_home[:14]}.csv", index=False
                )
            elif need_save and type_concat == "yearly":
                home_df.to_csv(
                    f"{data_path}_REPORT/{previous_home[:6]}.csv", index=False
                )


def compute_alert_reaction() -> NoReturn:
    """
    Function for reactions.
    """
    for community in COMMUNITY_NAME:
        print("--------------Computing Alerts--------------")
        path = RESAMPLED_FOLDER + '/' + community
        # For all file in the data folder
        i = -1
        previous_file = []
        for file in sorted(os.listdir(path)):
            print(f"---------------{file[:6]}---------------")
            df = pd.read_csv(path + '/' + file)
            df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
            df['day'] = pd.to_datetime(df['day'])
            df = df[df['p_cons'] > 0]
            # Find if a house reacted to the message
            if community == "CDB" and file[:6] in ALL_CDB:
                if file[:6] not in previous_file:
                    previous_file.append(file[:6])
                    i += 1
                find_reaction_report(
                    df, ALERTS_CDB, MATRIX_ALERTS_CDB, SUM_ALERTS_CDB, i
                )
            elif community == "ECH" and file[:6] in ALL_ECH:
                if file[:6] not in previous_file:
                    previous_file.append(file[:6])
                    i += 1
                find_reaction_report(
                    df, ALERTS_ECH, MATRIX_ALERTS_ECH, SUM_ALERTS_ECH, i
                )
    # Take all home ids and add (%)
    cdb_home_id = [f + ' (%)' for f in ALL_CDB]
    ech_home_id = [f + ' (%)' for f in ALL_ECH]

    # Export MATRIX_ALERTS_CDB or MATRIX_ALERTS_ECH in excel files
    export_to_XLSX(
        MATRIX_ALERTS_CDB, cdb_home_id, ALERTS_CDB, SUM_ALERTS_CDB, "plots/alerts_cdb.xlsx"
    )
    export_to_XLSX(
        MATRIX_ALERTS_ECH, ech_home_id, ALERTS_ECH, SUM_ALERTS_ECH, "plots/alerts_ech.xlsx"
    )


def plot_average(current_folder: str, fmt: str) -> NoReturn:
    """
    Plot average.

    :param current_folder:  Current folder where we are working (dataset or resample_dataset).
    :param fmt:             Data format.
    """
    starting = dt.datetime(2022, 11, 7, 0, 0, 0).astimezone()
    ending = dt.datetime(2022, 11, 14, 23, 59, 59).astimezone()
    # Plot an average for a given date for a community
    if AVERAGE_COMMUNITY:
        print("--------------Plotting average--------------")
        plot_average_community(
            starting, ending, current_folder, 5, fmt
        )
        plot_average_community(
            starting, ending, current_folder, 11, fmt
        )

    # Plot the average communities together
    if AVERAGE_COMMUNITIES:
        print("--------------Plotting average through communities--------------")
        for nb_selected_house in [5, 10, 15, 20]:
            print(f"--------------{nb_selected_house} selected houses--------------")
            average_through_community(
                starting, ending, current_folder, nb_selected_house, fmt
            )

    # Plot all aggregation
    if AGGREGATION:
        print("--------------Plotting Aggregation--------------")
        plot_aggregation(starting, ending, current_folder, fmt)


def plot_flukso() -> NoReturn:
    """
    Function to plot flukso data.
    """
    # Choose the format
    fmt = '8S' if SEC8 else '15min'
    current_folder = DATASET_FOLDER if SEC8 else RESAMPLED_FOLDER
    # For all communities
    if BASIC_PLOT or AREA_PLOT:
        for community in COMMUNITY_NAME:
            print("--------------Plotting--------------")
            # For all file in the data folder
            for file in sorted(os.listdir(current_folder + '/' + community)):
                print("---------------" + file[:6] + "---------------")
                df = pd.read_csv(current_folder + '/' + community + '/' + file)
                home_id = df.at[0, 'home_id']
                starting = dt.datetime(2022, 5, 17, 0, 0, 0).astimezone()
                ending = dt.datetime(2022, 5, 17, 23, 59, 59).astimezone()
                # Select the correct path according to the format (15min or 8S (for 8sec))
                if SEC8:
                    path = f"plots/{community}/{home_id}"
                    cdt = True
                else:
                    path = f"plots/{community}/{home_id}"
                    cdt = int(file[12:14]) == starting.month and int(file[7:11]) == starting.year
                if cdt and file[:6] not in [
                    'ECHA01', 'ECHASC', 'ECHBUA', 'ECHCOM', 'ECHL09', 'ECHL17'
                ]:
                    # Change it later because we will receive a correct df with the timezone
                    df['ts'] = (
                        pd.to_datetime(df['ts'])
                        .dt
                        .tz_localize(pytz.timezone('Europe/Brussels'), ambiguous=True)
                    )
                    if BASIC_PLOT:
                        plot_data(
                            df, path, starting, ending,
                            f'Home: {home_id}', 'multiple_flukso', fmt
                        )
                    elif AREA_PLOT:
                        plot_data(
                            df, path, starting, ending,
                            f'Home: {home_id}', 'flukso', fmt
                        )
    plot_average(current_folder, fmt)


def plot_rtu() -> NoReturn:
    """
    Function to plot rtu data.
    """
    if PLOT_MEDIAN_QUANTILE_RTU:
        df = pd.read_csv(f"{RESAMPLED_FOLDER}/RTU/rtu_15min.csv")
        df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        plot_median_quantile_rtu(df, 'plots/RTU')
    if PLOT_RANGE_RTU:
        df = pd.read_csv(RESAMPLED_FOLDER + '/RTU_REPORT/rtu_2022-12_15min.csv')
        starting = dt.datetime(2022, 12, 21, 0, 0, 0).astimezone()
        ending = dt.datetime(2022, 12, 21, 23, 59, 59).astimezone()
        plot_data(df, 'plots/RTU', starting, ending, 'Cabine basse tension', 'rtu')
    if MEAN_WED_RTU:
        time_series = (
            pd.date_range("00:00:00", freq='15min', periods=96)
            .to_series()
            .apply(lambda x: x.strftime('%H:%M:%S'))
            .reset_index(drop=True)
        )
        df = pd.read_csv(f"{RESAMPLED_FOLDER}/RTU/rtu_15min.csv")
        df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
        df = df[(df['ts'].dt.weekday == 2) & (df['ts'].dt.year != 2023)]
        not_during_light = df[~((df['ts'].dt.day == 21) & (df['ts'].dt.month == 12))]
        mean = df.groupby(not_during_light['ts'].dt.time).mean(numeric_only=True)
        rtu_plot(mean, time_series, 'plots/RTU', 'Mean wednesday RTU')


def all_plots() -> NoReturn:
    """
    Function to plot all data.
    """
    if FLUKSO:
        if PLOT_MEDIAN_QUANTILE_FLUKSO:
            for cdb in REPORT_CDB:
                df = pd.read_csv(f"{RESAMPLED_FOLDER}/CDB/{cdb}_15min.csv")
                # Add correct datetime with timezone.
                df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
                df = df[df['p_cons'] > 0]
                plot_median_quantile_flukso(
                    df, f"{RESAMPLED_FOLDER}/CDB", f"plots/CDB/{cdb}"
                )

            for ech in REPORT_ECH:
                df = pd.read_csv(f"{RESAMPLED_FOLDER}/ECH/{ech}_15min.csv")
                # Add correct datetime with timezone.
                df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
                plot_median_quantile_flukso(
                    df, f"{RESAMPLED_FOLDER}/ECH", f"plots/ECH/{ech}"
                )
        if MEAN_WED_FLUKSO:
            for cdb in ALL_CDB:
                time_series = (
                    pd.date_range("00:00:00", freq='15min', periods=96)
                    .to_series()
                    .apply(lambda x: x.strftime('%H:%M:%S'))
                    .reset_index(drop=True)
                )
                df = pd.read_csv(f"{RESAMPLED_FOLDER}/CDB/{cdb}_15min.csv")
                df['ts'] = pd.to_datetime(df['ts'], utc=True).dt.tz_convert(TZ)
                df = df[(df['ts'].dt.weekday == 2) & (df['ts'].dt.year != 2023)]
                df = df[df['p_cons'] > 0]
                df = df.resample('15min', on='ts').mean().reset_index()
                not_during_light = df[~((df['ts'].dt.day == 21) & (df['ts'].dt.month == 12))]
                mean = df.groupby(not_during_light['ts'].dt.time).mean(numeric_only=True)
                flukso_plot(mean, time_series, f"plots/CDB/{cdb}", f"Mean wednesday {cdb}")
    if RTU:
        plot_rtu()


def main() -> NoReturn:
    """
    Main function
    """
    # Manage the data (e.g. resample, etc.)
    if MANAGE_DATA:
        if FLUKSO:
            manage_flukso_data()
        if RTU:
            manage_rtu_data()

    # Concat small dataset in larger dataset
    if CONCAT_DATA:
        columns_name = ""
        if FLUKSO:
            columns_name = ['home_id', 'day', 'ts', 'p_cons', 'p_prod', 'p_tot']
        elif RTU:
            columns_name = [
                'ip', 'day', 'ts', 'active', 'apparent', 'cos_phi', 'reactive',
                'tension1_2', 'tension2_3', 'tension3_1'
            ]
        # yearly or monthly
        concat_data(columns_name, "yearly")

    # Compute and show the information about the alert
    if REACTION:
        compute_alert_reaction()

    # Plot
    if PLOT:
        all_plots()


if __name__ == "__main__":
    main()
