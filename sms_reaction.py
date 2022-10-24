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


def find_global_reaction_and_report(df, file_name, path, alerts, matrix, sum_alerts, index):
    """
    Function to find a reaction in the dataframe in a global point of view.

    :param df:          The dataframe.
    :param file_name:   Complete name of the file
    :param path:        Path to the file.
    :param alerts:      List of alerts.
    :param matrix:      The matrix containing values about consumption.
    :param sum_alerts:  List of all consumption during alerts - same period outside alerts.
    :param index:       Index of the home id.
    """
    for i in range(len(alerts)):
        starting_alert = datetime.datetime.strptime(alerts[i][0], '%Y-%m-%d %H:%M:%S')
        ending_alert = datetime.datetime.strptime(alerts[i][1], '%Y-%m-%d %H:%M:%S')
        if (int(file_name[7:11]) == starting_alert.year
                and int(file_name[12]) == starting_alert.month):
            nb_elem = 0
            mean_not_alert = 0
            sum_not_alert = 0
            months_home = []
            # Get the number of delta time used for the report
            nb_delta_alert = len(REPORTS_HOURS)
            # Compute the index according reported values
            # For each alert, we have:
            # A1 -12h | A1 -6h | A1 -3h | A1 | A1 3h | A1 6h | A1 12h |
            alert_idx = (i * 2 * nb_delta_alert) + (i + nb_delta_alert)
            # We recover all month for the current home
            for file2 in os.listdir(path):
                if file_name != file2:
                    if file_name[:6] == file2[:6]:
                        months_home.append(file2)
            # Sum all values before and after the period of the alert
            for k in [-1, 1]:
                tmp_sum, tmp_nb_elem = compute_sum_up_to_bound_and_count(
                    df, copy.deepcopy(months_home), k, alerts, starting_alert, ending_alert
                )
                if tmp_sum > 0:
                    sum_not_alert += tmp_sum
                    nb_elem += tmp_nb_elem
            # Mean during the alert
            alert_df = df.query(
                "ts >= \"" + str(starting_alert) + "\" and ts < \"" + str(ending_alert) + "\""
            )['p_cons']

            if not (alert_df <= 0).any().any():
                sum_alert = alert_df.sum()
                mean_alert = alert_df.mean()
                # Combined mean
                global_mean = (sum_alert + sum_not_alert) / (len(alert_df.index) + nb_elem)
                mean_not_alert = sum_not_alert / nb_elem
                # Compute the percentage
                matrix[index][alert_idx] = (
                    ((mean_alert - mean_not_alert) / global_mean)
                    * 100 if global_mean > 0 and mean_alert > 0 and mean_not_alert > 0 else 0
                )
                # Register the total sum of energy consumption in kWh
                if sum_alert > 0 and sum_not_alert > 0:
                    # We divide by 1000 because we want the results in kW and divide by 4
                    # to have the info in kWh
                    sum_alerts[alert_idx] += (
                        (sum_alert - (sum_not_alert * len(alert_df) / nb_elem)) / 4000
                    )

                # Find report
                find_report(df, alerts, months_home,  matrix, sum_alerts, index, i)


def compute_sum_up_to_bound_and_count(df, months_home, sign, alerts, starting_alert, ending_alert):
    """
    This function will compute the mean before or after the alert according to the sign

    :param df:              Dataframe
    :param months_home:     List of months for a home_id
    :param sign:            The sign used for the delta. -1 if we need to check
                            before the current date, 1 otherwise
    :param alerts:          All alerts
    :param starting_alert:  The beginning of the alert.
    :param ending_alert:    The end of the alert.

    :return:                Return the sum during the period and the number of elements.
    """
    current_period = starting_alert
    # The time between the beginning of the alert and the end of the alert
    delta_alert = ending_alert - starting_alert
    delta = datetime.timedelta(days=7 * sign)
    sum_period = 0
    count = 0
    tmp_df = df
    # For each file before or after (depending on the sign) the alert.
    while True:
        # Check if the date is not an alert.
        is_alert = False
        for i in range(len(alerts)):
            start = datetime.datetime.strptime(alerts[i][0], '%Y-%m-%d %H:%M:%S')
            end = datetime.datetime.strptime(alerts[i][1], '%Y-%m-%d %H:%M:%S')
            if ((start <= current_period and current_period <= end)
                    and (starting_alert <= current_period and current_period <= ending_alert)):
                is_alert = True
                break
        # If it is not an alert, we query the date and we do the sum
        if not is_alert:
            tmp_p_cons = (
                tmp_df
                .query(
                    "ts >= \"" + str(current_period) + "\" and ts < \""
                    + str(current_period + delta_alert) + "\""
                )['p_cons']
            )
            if not (tmp_p_cons <= 0).any().any():
                tmp_sum = tmp_p_cons.sum()
                # If the consumption is not missing or negative
                if tmp_sum > 0:
                    sum_period += tmp_sum
                    count += len(tmp_p_cons.index)
        # should be >= now
        if current_period + delta >= datetime.datetime.strptime("2022-08-12 15:45:00", '%Y-%m-%d %H:%M:%S'):
            break
        else:
            current_period += delta
            # check if the month is different
            if current_period.month != (current_period - delta).month:
                find = False
                file_name = ""
                # Search the following month
                for i in range(len(months_home)):
                    # Check if years are equal and check if months are equals,
                    # case where the month is one or two digit(s)
                    if ((int(months_home[i][7:11]) == int(current_period.year))
                        and
                        (int(months_home[i][12]) == int(current_period.month)
                            or months_home[i][12:14] == str(current_period.month))):
                        find = True
                        file_name = months_home[i]
                        del months_home[i]
                        break
                # If it is the case, we continue
                if find:
                    tmp_df = pd.read_csv(RESAMPLED_FOLDER + '/' + file_name[:3] + '/' + file_name)
                # Otherwise, there is no more file
                else:
                    break
    return sum_period, count


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
                starting_alert = (
                    datetime.datetime.strptime(alerts[index_j][0], '%Y-%m-%d %H:%M:%S')
                    + (-1 * REPORTS_HOURS[j])
                )
                ending_alert = datetime.datetime.strptime(alerts[index_j][1], '%Y-%m-%d %H:%M:%S')
            else:
                starting_alert = datetime.datetime.strptime(alerts[index_j][0], '%Y-%m-%d %H:%M:%S')
                ending_alert = (
                    datetime.datetime.strptime(alerts[index_j][1], '%Y-%m-%d %H:%M:%S')
                    + (REPORTS_HOURS[j])
                )
            # Compute the mean before and after the period of the alert
            nb_elem = 0
            mean_not_alert = 0
            sum_not_alert = 0
            # Check before and after the period
            for k in [-1, 1]:
                tmp_sum, tmp_nb_elem = compute_sum_up_to_bound_and_count(
                    df, copy.deepcopy(months_home), k, alerts, starting_alert, ending_alert
                )
                if tmp_sum > 0:
                    sum_not_alert += tmp_sum
                    nb_elem += tmp_nb_elem

            # Mean during the alert
            alert_df = df.query(
                "ts >= \"" + str(starting_alert) + "\" and ts < \"" + str(ending_alert) + "\""
            )['p_cons']
            sum_alert = alert_df.sum()
            mean_alert = alert_df.mean()
            # Compute global mean
            global_mean = (sum_alert + sum_not_alert) / (len(alert_df.index) + nb_elem)
            # Compute mean without the alert period
            mean_not_alert = sum_not_alert / nb_elem
            # Register the percentage of reduction
            matrix[index_i][alert_idx + (i * (j + 1))] = (
                ((mean_alert - mean_not_alert) / global_mean)
                * 100 if global_mean > 0 and mean_alert > 0 and mean_not_alert > 0 else 0
            )
            # Register the total sum of energy consumption in kWh
            if sum_alert > 0 and sum_not_alert > 0:
                # We divide by 1000 because we want the results in kW and divide by 4
                # to have the info in kWh
                sum_alerts[alert_idx + (i * (j + 1))] += (
                    (sum_alert - (sum_not_alert * len(alert_df) / nb_elem)) / 4000
                )


def compute_energy(series, dt):
    """
    Function to compute the energy where the formula of the evergy is:
    E = dt * sum(Powers)

    :param df:  Series containing powers.
    :param dt:  Derivative of time expressed in hour.

    :return:    Return the Wh
    """
    return series.sum() * dt


def check_data():
    """
    Test for data to be sure that we have correct data.
    """
    # ECHL05 : 4 mai 2022
    df = pd.read_csv('resampled_data/ECH/ECHL05_2022_5_15min.csv')
    dates = [
        [datetime.datetime(2022, 5, 4, 14, 00, 00), datetime.datetime(2022, 5, 4, 17, 00, 00)],
        [datetime.datetime(2022, 5, 11, 14, 00, 00), datetime.datetime(2022, 5, 11, 17, 00, 00)],
        [datetime.datetime(2022, 5, 18, 14, 00, 00), datetime.datetime(2022, 5, 18, 17, 00, 00)],
        [datetime.datetime(2022, 5, 25, 14, 00, 00), datetime.datetime(2022, 5, 25, 17, 00, 00)]
    ]
    tot_sum = 0
    for j in range(len(dates)):
        s_j = df.query(
            "ts >= \"" + str(dates[j][0]) + "\" and ts < \"" + str(dates[j][1]) + "\""
        )['p_cons']
        tmp = 0
        for i in range(len(dates)):
            if dates[j][0] != dates[i][0]:
                s_i = df.query(
                    "ts >= \"" + str(dates[i][0]) + "\" and ts < \"" + str(dates[i][1]) + "\""
                )['p_cons']
                tmp += compute_energy(s_i, 0.25) / (len(dates) - 1)
        tot_sum += (compute_energy(s_j, 0.25) - tmp)
    print(tot_sum)
