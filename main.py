__title__ = "main"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"

import datetime as dt
import os
import pandas as pd
import pytz

from plot_load_curves import (
    plot_average_community,
    average_through_community,
    plot_aggregation,
    plot_data
)
from sms_reaction import find_global_reaction_and_report
from utils import (
    check_negative_consumption, resample_dataset, export_to_XLSX
)

# Import constants for the configuration of the execution
from config import (
    FLUKSO,
    RTU,
    # Constants for path
    DATASET_FOLDER,
    RESAMPLED_FOLDER,
    # Constants for the name of communities
    COMMUNITY_NAME,
    # Constants for the data management
    MANAGE_DATA,
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
    # Variables for CDB
    ALERTS_CDB,
    MATRIX_ALERTS_CDB,
    SUM_ALERTS_CDB,
    # Variables for ECH
    ALERTS_ECH,
    MATRIX_ALERTS_ECH,
    SUM_ALERTS_ECH,
)


# ----------------------------- #
# ----------MAIN CODE---------- #
# ----------------------------- #


def manage_flukso_data():
    """
    Function to manage data.
    """
    # For all communities
    # for community in ['CDB']:
    for community in COMMUNITY_NAME:
        print("---------------Managing Data---------------")
        # For all file in the data folder
        # for file in sorted(os.listdir(community)):
        for file in sorted(os.listdir(DATASET_FOLDER + '/' + community)):
            print("---------------" + file[:6] + "---------------")
            # df = pd.read_csv(community + '/' + file)
            df = pd.read_csv(DATASET_FOLDER + '/' + community + '/' + file)

            # Check if there is a negative consumption
            if VERIFY_CONSUMPTION:
                check_negative_consumption(df, file[:6])

            # Run the resample function according to Resample boolean value
            if RESAMPLE:
                columns = ['home_id', 'day', 'ts', 'p_cons', 'p_prod', 'p_tot']
                resample_dataset(df, RESAMPLED_FOLDER + '/' + file[:3], file[:-4], columns)


def manage_rtu_data():
    """
    Manage rtu data.
    """
    # Resample rtu data
    if RESAMPLE_RTU:
        df = pd.read_csv('dataset/RTU/rtu.csv')
        columns = [
            'ip', 'day', 'ts', 'active',
            'apparent', 'cos_phi', 'reactive',
            'tension1_2', 'tension2_3', 'tension3_1'
        ]
        resample_dataset(df, RESAMPLED_FOLDER + '/RTU', 'rtu', columns)


def compute_alert_reaction():
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
            print(f"---------------{file[:6]} {file[7:14]} ---------------")
            df = pd.read_csv(path + '/' + file)
            # Find if a house reacted to the message
            if community == "CDB" and file[:6] in [
                'CDB002', 'CDB006', 'CDB008', 'CDB009',
                'CDB011', 'CDB014', 'CDB030', 'CDB033',
                'CDB036', 'CDB042', 'CDB043'
            ]:
                if file[:6] not in previous_file:
                    previous_file.append(file[:6])
                    i += 1
                find_global_reaction_and_report(
                    df, file, path, ALERTS_CDB, MATRIX_ALERTS_CDB, SUM_ALERTS_CDB, i
                )
            elif community == "ECH" and file[:6] in [
                'ECHL01', 'ECHL05', 'ECHL07',
                'ECHL08', 'ECHL11', 'ECHL12',
                'ECHL13', 'ECHL15', 'ECHL16'
            ]:
                if file[:6] not in previous_file:
                    previous_file.append(file[:6])
                    i += 1
                find_global_reaction_and_report(
                    df, file, path, ALERTS_ECH, MATRIX_ALERTS_ECH, SUM_ALERTS_ECH, i
                )

    # Take all home ids
    cdb_home_id = [
        'CDB002', 'CDB006', 'CDB008', 'CDB009', 'CDB011', 'CDB014',
        'CDB030', 'CDB033', 'CDB036', 'CDB042', 'CDB043'
    ]
    ech_home_id = [
        'ECHL01', 'ECHL05', 'ECHL07', 'ECHL08', 'ECHL11',
        'ECHL12', 'ECHL13', 'ECHL15', 'ECHL16'
    ]
    # cdb_home_id = [f[:6] for f in sorted(os.listdir(DATASET_FOLDER + '/CDB'))]
    # ech_home_id = [f[:6] for f in sorted(os.listdir(DATASET_FOLDER + '/ECH'))]

    # Export MATRIX_ALERTS_CDB or MATRIX_ALERTS_ECH in excel files
    export_to_XLSX(
        MATRIX_ALERTS_CDB, cdb_home_id, ALERTS_CDB, SUM_ALERTS_CDB, "plots/alerts_cdb.xlsx"
    )
    export_to_XLSX(
        MATRIX_ALERTS_ECH, ech_home_id, ALERTS_ECH, SUM_ALERTS_ECH, "plots/alerts_ech.xlsx"
    )


def plot_average(current_folder, fmt):
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


def plot_flukso():
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
                if cdt and file[:6] not in ['ECHA01', 'ECHASC', 'ECHBUA', 'ECHCOM', 'ECHL09', 'ECHL17']:
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


def plot_rtu():
    """
    Function to plot rtu data.
    """
    df = pd.read_csv(RESAMPLED_FOLDER + '/RTU/rtu_2022-11_15min.csv')
    starting = dt.datetime(2022, 11, 7, 0, 0, 0).astimezone()
    ending = dt.datetime(2022, 11, 14, 23, 59, 59).astimezone()
    plot_data(df, 'plots/RTU', starting, ending, 'Cabine basse tension', 'rtu')


def all_plots():
    """
    Function to plot all data.
    """
    if FLUKSO:
        plot_flukso()
    if RTU:
        plot_rtu()


def main():
    """
    Main function
    """
    # Manage the data (e.g. resample, etc.)
    if MANAGE_DATA:
        if FLUKSO:
            manage_flukso_data()

        if RTU:
            manage_rtu_data()

    # Compute and show the information about the alert
    if REACTION:
        compute_alert_reaction()

    # Plot
    if PLOT:
        all_plots()


if __name__ == "__main__":
    main()
