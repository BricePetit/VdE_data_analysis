__title__ = "main"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"

import os
import pandas as pd

from plot_load_curves import (
    plot_average_community,
    plot_basic_period,
    plot_basic_period_area,
    average_through_community,
    plot_aggregation
)
from sms_reaction import find_global_reaction_and_report
from utils import (
    check_negative_consumption, utc_to_cet, resample_dataset, export_to_XLSX, check_mistake
)

# Import constants for the configuration of the execution
from config import (
    # Constants for path
    DATASET_FOLDER,
    RESAMPLED_FOLDER,
    # Constants for the name of communities
    COMMUNITY_NAME,
    # Constants for the data management
    MANAGE_DATA,
    VERIFY_CONSUMPTION,
    CONVERT_UTC_CET,
    RESAMPLE,
    INCONSISTENCY,
    # Constants for reactions of messages
    REACTION,
    GLOBAL_REACTION,
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
    ALERT_REACTION_CDB,
    RANKING_ALERT_CDB,
    MATRIX_ALERTS_CDB,
    SUM_ALERTS_CDB,
    # Variables for ECH
    ALERTS_ECH,
    ALERT_REACTION_ECH,
    RANKING_ALERT_ECH,
    MATRIX_ALERTS_ECH,
    SUM_ALERTS_ECH,
)


# ----------------------------- #
# ----------MAIN CODE---------- #
# ----------------------------- #


def manage_data():
    """
    Function to manage data.
    """
    # For all communities
    for community in COMMUNITY_NAME:
        print("---------------Managing Data---------------")
        # For all file in the data folder
        for file in os.listdir(DATASET_FOLDER + '/' + community):
            print("---------------" + file[:6] + "---------------")
            df = pd.read_csv(DATASET_FOLDER + '/' + community + '/' + file)

            # Check if there is a negative consumption
            if VERIFY_CONSUMPTION:
                check_negative_consumption(df, file[:6])

            # Check if we need to apply the correction
            if CONVERT_UTC_CET:
                utc_to_cet(df, file, community)

            # Run the resample function according to Resample boolean value
            if RESAMPLE:
                resample_dataset(file, df)


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
        for file in os.listdir(path):
            print("---------------" + file[:6] + "---------------")
            df = pd.read_csv(path + '/' + file)
            # Find if a house reacted to the message
            if GLOBAL_REACTION:
                if community == "CDB" and file[:6] in [
                    'CDB002', 'CDB006', 'CDB008', 'CDB009',
                    'CDB011', 'CDB014', 'CDB030', 'CDB033',
                    'CDB036', 'CDB042', 'CDB043'
                ]:
                    if file[:6] not in previous_file:
                        previous_file.append(file[:6])
                        i += 1
                    find_global_reaction_and_report(
                        df, file, path, ALERTS_CDB, ALERT_REACTION_CDB, RANKING_ALERT_CDB,
                        MATRIX_ALERTS_CDB, SUM_ALERTS_CDB, i
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
                        df, file, path, ALERTS_ECH, ALERT_REACTION_ECH, RANKING_ALERT_ECH,
                        MATRIX_ALERTS_ECH, SUM_ALERTS_ECH, i
                    )

    print()
    print("----------CDB ALERTS RANKING----------")
    for key in RANKING_ALERT_CDB:
        print(
            "The alert", key, f"{ALERTS_CDB[key]}", "obtained", RANKING_ALERT_CDB[key],
            "reactions."
        )
    print()
    print("----------CDB ALERTS RANKING----------")
    for key in ALERT_REACTION_CDB:
        print(
            f"The home id {key} reacted to the following alerts {ALERT_REACTION_CDB[key]}."
        )
        print(
            f"The home id {key} reacted to {len(ALERT_REACTION_CDB[key])/len(ALERTS_CDB)*100}"
            + "% of the alerts."
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
    # cdb_home_id = [f[:6] for f in os.listdir(DATASET_FOLDER + '/CDB')]
    # ech_home_id = [f[:6] for f in os.listdir(DATASET_FOLDER + '/ECH')]

    # Export MATRIX_ALERTS_CDB or MATRIX_ALERTS_ECH in excel files
    export_to_XLSX(
        MATRIX_ALERTS_CDB, cdb_home_id, ALERTS_CDB, SUM_ALERTS_CDB, "plots/alerts_cdb.xlsx"
    )
    export_to_XLSX(
        MATRIX_ALERTS_ECH, ech_home_id, ALERTS_ECH, SUM_ALERTS_ECH, "plots/alerts_ech.xlsx"
    )


def all_plots():
    """
    Function to plot.
    """
    # Choose the format
    fmt = '8S' if SEC8 else '15min'
    current_folder = DATASET_FOLDER if SEC8 else RESAMPLED_FOLDER
    # For all communities
    if BASIC_PLOT or AREA_PLOT:
        for community in COMMUNITY_NAME:
            print("--------------Plotting--------------")
            # For all file in the data folder
            for file in os.listdir(current_folder + '/' + community):
                print("---------------" + file[:6] + "---------------")
                df = pd.read_csv(current_folder + '/' + community + '/' + file)
                home_id = df.at[0, 'home_id']
                # Select the correct path according to the format (15min or 8S (for 8sec))
                if SEC8:
                    path = f"plots/{community}/{home_id}"
                else:
                    path = f"plots/{community}/{home_id}"
                if BASIC_PLOT:
                    # if int(file[12]) == 5 and int(file[7:11]) == 2022 and not :
                    plot_basic_period(
                        df, path, home_id, "2022-05-24 00:00:00", "2022-05-24 23:59:52", fmt
                    )
                elif AREA_PLOT:
                    plot_basic_period_area(
                        df, path, home_id, "2022-05-24 00:00:00", "2022-05-24 23:59:52", fmt
                    )
    # Plot an average for a given date for a community
    if AVERAGE_COMMUNITY:
        print("--------------Plotting average--------------")
        plot_average_community(
            "2022-05-24 00:00:00", "2022-05-24 23:59:52", current_folder, 5, fmt
        )
        plot_average_community(
            "2022-05-24 00:00:00", "2022-05-24 23:59:52", current_folder, 11, fmt
        )

    # Plot the average communities together
    if AVERAGE_COMMUNITIES:
        print("--------------Plotting average through communities--------------")
        for nb_selected_house in [5, 10, 15, 20]:
            print(f"--------------{nb_selected_house} selected houses--------------")
            average_through_community(
                "2022-05-24 00:00:00", "2022-05-24 23:59:52", current_folder, nb_selected_house, fmt
            )

    # Plot all aggregation
    if AGGREGATION:
        print("--------------Plotting Aggregation--------------")
        plot_aggregation("2022-05-24 00:00:00", "2022-05-24 23:59:52", current_folder, fmt)


def main():
    """
    Main function
    """
    # Manage the data (e.g. resample, etc.)
    if MANAGE_DATA:
        manage_data()

    # Check the inconsistency in data
    if INCONSISTENCY:
        check_mistake()

    # Compute and show the information about the alert
    if REACTION:
        compute_alert_reaction()

    # Plot
    if PLOT:
        all_plots()


if __name__ == "__main__":
    main()
