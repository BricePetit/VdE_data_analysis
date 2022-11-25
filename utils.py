__title__ = "utils"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


import datetime as dt
import numpy as np
import os
import pandas as pd
import pytz


from config import (
    FLUKSO,
    RTU,
    REPORTS_HOURS
)


def check_negative_consumption(df, home_id):
    """
    Check if there is negative consumption in the dataframe. It will print the home_id and indexes
    of the dataframe.
    """
    if (df['p_cons'] < 0).any().any():
        print()
        print("/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\")
        print("Home: " + home_id)
        print(df.index[df['p_cons'] < 0])
        print("/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\")
        print()


def convert_utc_to_local_tz(series):
    """
    Convert a series containing a timestamp with a specific timezone to the local timezone.
    In this function, we remove microseconds and we convert utc to the local time zone.

    :series:    Series.

    :return:    Return a series with the local timezone.
    """
    tz = pytz.timezone('Europe/Brussels')
    return pd.to_datetime(series, utc=True).dt.round(freq='s').dt.tz_convert(tz)


def adjust_timestamp(initial_ts):
    """
    Function to adjust the initial timestamp in order to start at 00, 15, 30 or 45.

    :param initial_ts:  Initial timestamp.

    :return:            Adjusted initial timestamp.
    """
    if initial_ts.second % 60 != 00:
        initial_ts += dt.timedelta(seconds=(60 - (initial_ts.second % 60)))
        if initial_ts.minute % 15 != 00:
            initial_ts += dt.timedelta(minutes=(15 - (initial_ts.minute % 15)))
    return initial_ts


def create_average_df_flukso(df, current_ts, columns):
    """
    Create the flukso dataframe with the averaged value for each columns.

    :param df:          Dataframe.
    :param current_ts:  Current timestamp.
    :param columns:     List of column names.

    :return:            Series with averaged values.
    """
    home_id = df['home_id'].iloc[0]
    day = df['day'].iloc[0]
    average_p_cons = round(df['p_cons'].mean(), 0)
    average_p_prod = round(df['p_prod'].mean(), 0)
    average_p_tot = round(df['p_tot'].mean(), 0)
    return pd.DataFrame(
        [[home_id, day, current_ts, average_p_cons, average_p_prod, average_p_tot]],
        columns=columns
    )


def create_average_df_rtu(df, current_ts, columns):
    """
    Create the rtu dataframe with the averaged value for each columns.

    :param df:          Dataframe.
    :param current_ts:  Current timestamp.
    :param columns:     List of column names.

    :return:            Series with averaged values.
    """
    average_active = round(df['active'].mean(), 0)
    average_apparent = round(df['apparent'].mean(), 0)
    average_cos_phi = round(df['cos_phi'].mean(), 4)
    average_reactive = round(df['reactive'].mean(), 0)
    average_tension1_2 = round(df['tension1_2'].mean(), 4)
    average_tension2_3 = round(df['tension2_3'].mean(), 4)
    average_tension3_1 = round(df['tension3_1'].mean(), 4)
    return pd.DataFrame(
        [[
            df['ip'].iloc[0], df['day'].iloc[0], current_ts, average_active,
            average_apparent, average_cos_phi, average_reactive,
            average_tension1_2, average_tension2_3, average_tension3_1
        ]],
        columns=columns
    )


def resample_dataset(df, path, file_name, columns, fmt=15):
    """
    Function to resample data into 15 minutes data.

    :param df:          Dataframe.
    :param path:        Path to save the file.
    :param file_name:   Filename
    :param columns:     List of column names.
    :param fmt=15:      Format of the dataset.
    """
    # Convert the timezone
    df['ts'] = convert_utc_to_local_tz(df['ts'])
    # Create the new dataframe
    new_df = pd.DataFrame(columns=columns)
    # Init time
    initial_ts = adjust_timestamp(df['ts'][0])
    last_ts = df['ts'].iloc[-1]
    current_ts = initial_ts
    delta = dt.timedelta(minutes=fmt)
    if not os.path.isdir(path):
        os.makedirs(path)
    # Loop until we parsed the entire file
    while current_ts < last_ts:
        tmp_df = df.query(
            "ts >= \"" + str(current_ts) + "\" and ts < \"" + str(current_ts + delta) + "\""
        )
        # Check if the df is not empty because it is possible to obtain a gap between periods
        if tmp_df.size != 0:
            if FLUKSO:
                new_df = pd.concat(
                    [new_df, create_average_df_flukso(tmp_df, current_ts, columns)],
                    ignore_index=True
                )
            elif RTU:
                new_df = pd.concat(
                    [new_df, create_average_df_rtu(tmp_df, current_ts, columns)],
                    ignore_index=True
                )
            # If the next 15 minutes are in another month, we will save the current month.
            if current_ts.month != (current_ts + delta).month:
                month = str(current_ts.month)
                month = "0" + month if len(month) == 1 else month
                new_df.to_csv(
                    path + '/' + file_name + '_' + str(current_ts.year) + "-"
                    + month + '_' + str(fmt) + 'min.csv', index=False
                )
                new_df = pd.DataFrame(columns=columns)
        # Update the timestamp
        current_ts = current_ts + delta
    # Write the sampled dataset
    month = str(current_ts.month)
    month = "0" + month if len(month) == 1 else month
    new_df.to_csv(
        path + '/' + file_name + '_' + str(current_ts.year) + "-" + month
        + '_' + str(fmt) + 'min.csv', index=False
    )


def export_to_XLSX(matrix, home_ids, alerts, sum_alerts, file_name):
    """
    Export the dataframe in a excel file.

    :param matrix:      Matrix containing the results of alerts.
    :param home_ids:    The id of each home.
    :param alerts:      Dataframe with alerts.
    :param sum_alerts:  List of all consumption during alerts - same period outside alerts.
    :param file_name:   Filename.
    """
    # Week of the day
    weekday = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
    months = [
        'Janvier', 'Février', 'Mars', 'Avril',
        'Mai', 'Juin', 'Juillet', 'Aout',
        'Septembre', 'Octobre', 'Novembre', 'Decembre'
    ]
    # Create empty list of string
    alerts_name = ["" for _ in range(len(matrix[0]))]
    # Get the number of delta time used for the report
    nb_delta_alert = len(REPORTS_HOURS)
    for j in range(len(alerts)):
        # Compute the index according reported values
        # For each alert, we have:
        # A1 -12h | A1 -6h | A1 -3h | A1 | A1 3h | A1 6h | A1 12h |
        alert_idx = (j * 2 * nb_delta_alert) + (j + nb_delta_alert)
        # We used the -1 and 1 to go before/after the alert
        for k in [-1, 1]:
            # For each delta time
            for m in range(nb_delta_alert):
                # Write values Aj -12h | Aj -6h | Aj -3h | Aj 3h | Aj 6h | Aj 12h |
                # Where j is the number of the alert
                sep = '' if k == -1 else '+'
                alerts_name[alert_idx + (k * (m + 1))] = (
                    'A' + str(j + 1) + sep + str(int(REPORTS_HOURS[m].seconds / 3600) * k) + 'h'
                )
            # Write value A
            str_alert = ""
            starting_date = (
                dt.datetime.fromtimestamp(dt.datetime.timestamp(alerts.iloc[j][1])).astimezone()
            )
            ending_date = (
                dt.datetime.fromtimestamp(dt.datetime.timestamp(alerts.iloc[j][2])).astimezone()
            )
            str_alert += weekday[starting_date.weekday()]
            str_alert += str(starting_date.day) + ' '
            str_alert += months[starting_date.month - 1] + ' '
            str_alert += str(starting_date.hour) + 'h - ' + str(ending_date.hour) + 'h'

            alerts_name[alert_idx] = str_alert
    # Create the dataframe with specific indexes and column name
    df = pd.DataFrame(data=np.array(matrix), index=home_ids, columns=alerts_name)
    # Create a dataframe with the "Bilan"
    tmp_df = pd.DataFrame(data=[sum_alerts], index=["Bilan en kWh"], columns=alerts_name)
    # Concatenate the two dataframes
    df = pd.concat([df, tmp_df])
    # Write the dataframe into an xlsx file
    df.to_excel(excel_writer=file_name)
    print("Data exported in the file !")
