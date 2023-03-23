__title__ = "utils"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


import datetime as dt
import numpy as np
import os
import pandas as pd
from typing import NoReturn, List, Dict


from config import (
    TZ,
    REPORTS_HOURS
)


def check_negative_consumption(df: pd.DataFrame, home_id: str) -> NoReturn:
    """
    Check if there is negative consumption in the dataframe. It will print the home_id and indexes
    of the dataframe.

    :param df:      Dataframe.
    :param home_id: The id of the home.
    """
    if (df['p_cons'] < 0).any().any():
        print()
        print("/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\")
        print("Home: " + home_id)
        print(df.index[df['p_cons'] < 0])
        print("/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\")
        print()


def resample_dataset(
    df: pd.DataFrame, path: str, filename: str, agg: Dict[str, str], fmt='15min'
) -> NoReturn:
    """
    Function to resample data into 15 minutes data.

    :param df:          Dataframe.
    :param path:        Path to save the file.
    :param filename:    Filename.
    :param agg:         Aggregation to apply for the resample.
    :param fmt='15min': Format of the dataset.
    """
    print(f"--------------------Processing file {filename}--------------------")
    if not os.path.isdir(path):
        os.makedirs(path)
    df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts'], utc=True)
    df: pd.DataFrame = (
        df
        .resample(fmt, on='ts')
        .agg(agg)
        .reset_index()
    )
    df['ts']: pd.TimestampSeries = pd.to_datetime(df['ts']).dt.tz_convert(TZ)
    df.to_csv(path + '/' + filename + '_' + fmt + '.csv', index=False)
    print(f"--------------------{filename} saved--------------------")


def export_to_XLSX(
    matrix: np.NDArray[np.float64],
    home_ids: List[str],
    alerts: pd.DataFrame,
    sum_alerts: np.NDArray[np.float64],
    file_name: str
) -> NoReturn:
    """
    Export the dataframe in a excel file.

    :param matrix:      Matrix containing the results of alerts.
    :param home_ids:    The id of each home.
    :param alerts:      Dataframe with alerts.
    :param sum_alerts:  List of all consumption during alerts - same period outside alerts.
    :param file_name:   Filename.
    """
    print(f"Exporting data of {home_ids[0][:3]}...")
    # Week of the day
    weekday: List[str] = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
    months: List[str] = [
        'Janvier', 'Février', 'Mars', 'Avril',
        'Mai', 'Juin', 'Juillet', 'Aout',
        'Septembre', 'Octobre', 'Novembre', 'Decembre'
    ]
    # Create empty list of string
    alerts_name: List[str] = ["" for _ in range(len(matrix[0]))]
    # Get the number of delta time used for the report
    nb_delta_alert: int = len(REPORTS_HOURS)
    for j in range(len(alerts)):
        # Compute the index according reported values
        # For each alert, we have:
        # A1 -12h | A1 -6h | A1 -3h | A1 | A1 3h | A1 6h | A1 12h |
        alert_idx: int = (j * 2 * nb_delta_alert) + (j + nb_delta_alert)
        # We used the -1 and 1 to go before/after the alert
        for k in [-1, 1]:
            # For each delta time
            for m in range(nb_delta_alert):
                # Write values Aj -12h | Aj -6h | Aj -3h | Aj 3h | Aj 6h | Aj 12h |
                # Where j is the number of the alert
                sep: str = '' if k == -1 else '+'
                alerts_name[alert_idx + (k * (m + 1))] = (
                    sep + str(int(REPORTS_HOURS[m].seconds / 3600) * k) + 'h'
                )
            # Write value A
            str_alert: str = ""
            starting_date: dt.datetime = (
                dt.datetime.fromtimestamp(dt.datetime.timestamp(alerts.iloc[j][1])).astimezone()
            )
            ending_date: dt.datetime = (
                dt.datetime.fromtimestamp(dt.datetime.timestamp(alerts.iloc[j][2])).astimezone()
            )
            str_alert += f"{weekday[starting_date.weekday()]}"
            str_alert += f" {str(starting_date.day)} \n"
            str_alert += f"{months[starting_date.month - 1]} "
            str_alert += f"{str(starting_date.hour)}h - {str(ending_date.hour)}h"

            alerts_name[alert_idx] = str_alert
    # Create the dataframe with specific indexes and column name
    df: pd.DataFrame = pd.DataFrame(data=np.array(matrix), index=home_ids, columns=alerts_name)
    # Create a dataframe with the "Bilan"
    tmp_df: pd.DataFrame = pd.DataFrame(data=[sum_alerts], index=["Bilan en Wh"], columns=alerts_name)
    # Concatenate the two dataframes
    df: pd.DataFrame = pd.concat([df, tmp_df])
    # Write the dataframe into an xlsx file
    df.to_excel(excel_writer=file_name)
    print("Data exported in the file !")
