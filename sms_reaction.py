__title__ = "smsReaction"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


from config import (
    RESAMPLED_FOLDER,
    REPORTS_HOURS
)


import copy
import datetime
import os
import pandas as pd


# -------------------------------------- #
# ----------REACTION FUNCTIONS---------- #
# -------------------------------------- #


def find_global_reaction_and_report(df, file_name, path, alerts, reaction, ranking, matrix,
                                    sum_alerts, index):
    """
    Function to find a reaction in the dataframe in a global point of view.

    :param df:          The dataframe.
    :param file_name:   Complete name of the file
    :param path:        Path to the file.
    :param alerts:      List of alerts.
    :param reaction:    List of reactions.
    :param ranking:     Ranking of each reaction in a list.
    :param matrix:      The matrix containing values about consumption.
    :param sum_alerts:  List of all consumption during alerts - same period outside alerts.
    :param index:       Index of the home id.
    """
    for i in range(len(alerts)):
        starting_alert = datetime.datetime.strptime(alerts[i][0], '%Y-%m-%d %H:%M:%S')
        ending_alert = datetime.datetime.strptime(alerts[i][1], '%Y-%m-%d %H:%M:%S')
        if (int(file_name[7:11]) == starting_alert.year
                and int(file_name[12]) == starting_alert.month):
            months_home = []
            # We recover all month for the current home
            for file2 in os.listdir(path):
                if file_name != file2:
                    if file_name[:6] == file2[:6]:
                        months_home.append(file2)
            # Compute the mean before and after the period of the alert
            count = 0
            mean = 0
            sum_period = 0
            for k in [-1, 1]:
                tmp_mean, tmp_sum = compute_mean_sum_up_to_bound(
                    df, copy.deepcopy(months_home), k, alerts, starting_alert, ending_alert
                )
                if tmp_mean > 0:
                    count += 1
                    mean += tmp_mean
                if tmp_sum > 0:
                    sum_period += tmp_sum
            # Mean during the alert
            alert_df = df.query(
                "ts >= \"" + str(starting_alert) + "\" and ts < \"" + str(ending_alert) + "\""
            )['p_cons']
            mean_alert = alert_df.mean()
            count += 1
            mean = (mean + mean_alert) / count
            # We check if the mean is lower than during the alert
            if mean_alert < mean:
                ranking[i] = ranking[i] + 1 if i in ranking else 1
                if file_name[:6] in reaction:
                    reaction[file_name[:6]].append(i)
                else:
                    reaction[file_name[:6]] = [i]
            # Get the number of delta time used for the report
            nb_delta_alert = len(REPORTS_HOURS)
            # Compute the index according reported values
            # For each alert, we have:
            # A1 -12h | A1 -6h | A1 -3h | A1 | A1 3h | A1 6h | A1 12h |
            alert_idx = (i * 2 * nb_delta_alert) + (i + nb_delta_alert)
            # Compute the percentage
            matrix[index][alert_idx] = (
                ((mean_alert - mean) / mean) * 100 if mean > 0 and mean_alert > 0 else 0
            )
            # Compute the sum
            if alert_df.sum() > 0 and sum_period > 0:
                sum_alerts[alert_idx] += (alert_df.sum() - sum_period) / 1000

            # Find report
            find_report(df, alerts, months_home,  matrix, sum_alerts, index, i)


def compute_mean_sum_up_to_bound(df, months_home, sign, alerts, starting_alert, ending_alert):
    """
    This function will compute the mean before or after the alert according to the sign

    :param df:              Dataframe
    :param months_home:     List of months for a home_id
    :param sign:            The sign used for the delta. -1 if we need to check
                            before the current date, 1 otherwise
    :param alerts:          All alerts
    :param starting_alert:  The beginning of the alert.
    :param ending_alert:    The end of the alert.

    :return:                Return the mean and the sum during the period.
    """
    finished = False
    current_period = starting_alert
    # The time between the beginning of the alert and the end of the alert
    delta_alert = ending_alert - starting_alert
    delta = datetime.timedelta(days=7 * sign)
    sum_period = 0
    count = 0
    tmp_df = df
    # For each file before or after (depending on the sign) the alert.
    while not finished:
        # Check if the date is not an alert.
        is_alert = False
        for i in range(len(alerts)):
            start = datetime.datetime.strptime(alerts[i][0], '%Y-%m-%d %H:%M:%S')
            end = datetime.datetime.strptime(alerts[i][1], '%Y-%m-%d %H:%M:%S')
            if start <= current_period and current_period <= end:
                is_alert = True
                break
        if not is_alert:
            # check if the month is the same
            if current_period.month == (current_period - delta).month:
                tmp_mean = tmp_df.query(
                    "ts >= \"" + str(current_period) + "\" and ts < \""
                    + str(current_period + delta_alert) + "\""
                )['p_cons'].mean()
                # If the consumption is not missing or negative
                if tmp_mean > 0:
                    sum_period += tmp_mean
                    count += 1
            else:
                find = False
                # Search the following month
                for i in range(len(months_home)):
                    # Check if years are equal
                    if int(months_home[i][7:11]) == int(current_period.year):
                        # Check if months are equals, case where the month is one or two digit(s)
                        if (int(months_home[i][12]) == int(current_period.month)
                                or months_home[i][12:14] == str(current_period.month)):
                            find = True
                            file_name = months_home[i]
                            del months_home[i]
                            break
                # If it is the case, we continue
                if find:
                    tmp_df = pd.read_csv(RESAMPLED_FOLDER + '/' + file_name[:3] + '/' + file_name)
                    tmp_mean = tmp_df.query(
                        "ts >= \"" + str(current_period) + "\" and ts < \""
                        + str(current_period + delta_alert) + "\""
                    )['p_cons'].mean()
                    # If the consumption is not missing or negative
                    if tmp_mean > 0:
                        sum_period += tmp_mean
                        count += 1
                # Otherwise, there is no more file
                else:
                    finished = True
        # should be >= now
        if current_period + delta >= datetime.datetime.strptime("2022-08-12 15:45:00", '%Y-%m-%d %H:%M:%S'):
            finished = True
        else:
            current_period += delta
    return sum_period / (1 if count == 0 else count), sum_period


def find_report(df, alerts, months_home, matrix, sum_alerts, index_i, index_j):
    """
    This function will find a report of the consumption before/after the alert.

    :param df:          Dataframe
    :param alerts:      All alerts.
    :param months_home: List of months for a home_id
    :param matrix:      The matrix containing values about consumption.
    :param sum_alerts:  List of all consumption during alerts - same period outside alerts.
    :param index_i:     The index of the home_id.
    :param index_j:     The index of the alert.

    :return:                Return the mean.
    """
    # Get the number of delta time used for the report
    nb_delta_alert = len(REPORTS_HOURS)
    # Compute the index according reported values
    # For each alert, we have:
    # A1 -12h | A1 -6h | A1 -3h | A1 | A1 3h | A1 6h | A1 12h |
    alert_idx = (index_j * 2 * nb_delta_alert) + (index_j + nb_delta_alert)
    # We used the -1 and 1 to go before/after the alert
    for i in [-1, 1]:
        # For each delta time
        for j in range(nb_delta_alert):
            # Convert ts string into datetime
            if i == -1:
                starting_alert = datetime.datetime.strptime(
                    alerts[index_j][0], '%Y-%m-%d %H:%M:%S'
                ) + (-1 * REPORTS_HOURS[j])
                ending_alert = datetime.datetime.strptime(alerts[index_j][0], '%Y-%m-%d %H:%M:%S')
            else:
                starting_alert = datetime.datetime.strptime(alerts[index_j][1], '%Y-%m-%d %H:%M:%S')
                ending_alert = (
                    datetime.datetime.strptime(alerts[index_j][1], '%Y-%m-%d %H:%M:%S')
                    + (REPORTS_HOURS[j])
                )
            # Compute the mean before and after the period of the alert
            count = 0
            mean = 0
            sum_period = 0
            for k in [-1, 1]:
                tmp_mean, tmp_sum = compute_mean_sum_up_to_bound(
                    df, copy.deepcopy(months_home), k, alerts, starting_alert, ending_alert
                )
                if tmp_mean > 0:
                    count += 1
                    mean += tmp_mean
                if tmp_sum > 0:
                    sum_period += tmp_sum

            # Mean during the alert
            alert_df = df.query(
                "ts >= \"" + str(starting_alert) + "\" and ts < \"" + str(ending_alert) + "\""
            )['p_cons']
            mean_alert = alert_df.mean()
            count += 1
            mean = (mean + mean_alert) / count
            matrix[index_i][alert_idx + (i * (j + 1))] = (
                ((mean_alert - mean) / mean) * 100 if mean > 0 and mean_alert > 0 else 0
            )
            if alert_df.sum() > 0 and sum_period > 0:
                sum_alerts[alert_idx + (i * (j + 1))] += (alert_df.sum() - sum_period) / 1000
