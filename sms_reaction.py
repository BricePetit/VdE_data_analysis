__title__ = "smsReaction"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


from config import (
    REPORTS_HOURS
)


import datetime as dt
import numpy as np
import pandas as pd
from typing import NoReturn, List, Tuple


# -------------------------------------- #
# ----------REACTION FUNCTIONS---------- #
# -------------------------------------- #


def find_reaction_report(
    df: pd.DataFrame,
    alerts: pd.DataFrame,
    matrix,
    sum_alerts,
    index: int
) -> NoReturn:
    """
    Function to compute the reaction and the report.

    :param df:          Dataframe.
    :param alerts:      Dataframe with alerts.
    :param matrix:      Matrix with the result of report and reaction.
    :param sum_alerts:  Matrix with the sum during alerts and not.
    :param index:       The index of the home.
    """
    for i in range(len(alerts.index)):
        start_alert = (
            dt.datetime.fromtimestamp(dt.datetime.timestamp(alerts.iloc[i][1])).astimezone()
        )
        end_alert = (
            dt.datetime.fromtimestamp(dt.datetime.timestamp(alerts.iloc[i][2])).astimezone()
        )
        nb_hours = ((end_alert - start_alert) / 3600).seconds
        nb_delta_alert = len(REPORTS_HOURS)
        alert_idx = (i * 2 * nb_delta_alert) + (i + nb_delta_alert)
        # Group according to day of the week, 0 is Monday.
        grouped_df = df.groupby(df['ts'].dt.weekday)
        # Take the correct group according to the day of the alert.
        days_df = grouped_df.get_group(start_alert.weekday())
        # Keep only the period of the alert.
        days_df = (
            days_df[
                (start_alert.time() <= days_df['ts'].dt.time)
                & (days_df['ts'].dt.time < end_alert.time())
            ]
        )
        # Take the day of the alert and keep others
        alert_df = days_df[days_df['day'].dt.date == start_alert.date()]['p_cons']
        # Remove the day of the alert and keep others
        not_alert = days_df[days_df['day'].dt.date != start_alert.date()]['p_cons']
        # Sum for alert and non alert data
        sum_alert = alert_df.sum()
        sum_not_alert = not_alert.sum()
        # Mean for alert and non alert data
        mean_alert = alert_df.mean()
        mean_not_alert = not_alert.mean()
        # Combined mean
        global_mean = (sum_alert + sum_not_alert) / (len(alert_df.index) + len(not_alert.index))
        # Compute the percentages
        matrix[index][alert_idx] = (
            ((mean_alert - mean_not_alert) / global_mean)
            * 100
        )
        # Register the total sum of energy consumption in kWh
        if sum_alert > 0 and sum_not_alert > 0:
            sum_alerts[alert_idx] += (
                (sum_alert - (sum_not_alert * len(alert_df.index) / len(not_alert.index)))
                / nb_hours
            )
        # Find report
        find_report(
            df, matrix, sum_alerts, index, start_alert, end_alert, nb_delta_alert, alert_idx
        )


def find_report(
    df: pd.DataFrame,
    matrix,
    sum_alerts,
    index_i: int,
    start_alert,
    end_alert,
    nb_delta_alert: int,
    alert_idx: int
) -> NoReturn:
    """
    Function to compute the report.

    :param df:              Dataframe.
    :param matrix:          Matrix with the result of report and reaction.
    :param sum_alerts:      Matrix with the sum during alerts and not.
    :param index_i:         The index of the home.
    :param start_alert:     Start of the alert.
    :param end_alert:       End of the alert.
    :param nb_delta_alert:  Number of reports.
    :param alert_idx:       Index of the alert.
    """
    # We used the -1 and 1 to go before/after the alert
    for i in [-1, 1]:
        # For each delta time
        for j in range(nb_delta_alert):
            if i == -1:
                start_alert_report = start_alert - REPORTS_HOURS[j]
                end_alert_report = start_alert
            else:
                start_alert_report = end_alert
                end_alert_report = end_alert + REPORTS_HOURS[j]
            # Group according to day of the week, 0 is Monday.
            grouped_df = df.groupby(df['ts'].dt.weekday)
            # Take the correct group according to the day of the alert.
            if start_alert_report.weekday() != end_alert_report.weekday():
                day1 = (
                    df[
                        (df["ts"].dt.weekday == start_alert_report.weekday())
                        & (df["ts"].dt.hour >= start_alert_report.hour)
                        & (df["ts"].dt.hour <= 23)
                    ]
                )
                day2 = (
                    df[
                        (df["ts"].dt.weekday == end_alert_report.weekday())
                        & (df["ts"].dt.hour >= 0)
                        & (df["ts"].dt.hour < end_alert_report.hour)
                    ]
                )
                days_df = pd.concat([day1, day2]).sort_values(by='ts')
                # Take the day of the alert and keep others
                alert_df = days_df[
                    (days_df['day'].dt.date == start_alert_report.date())
                    | (days_df['day'].dt.date == end_alert_report.date())
                ]['p_cons']
                # Remove the day of the alert and keep others
                not_alert = days_df[
                    (days_df['day'].dt.date != start_alert_report.date())
                    | (days_df['day'].dt.date != end_alert_report.date())
                ]['p_cons']
            else:
                days_df = grouped_df.get_group(start_alert_report.weekday())
                # Keep only the period of the alert.
                days_df = (
                    days_df[
                        (start_alert_report.time() <= days_df['ts'].dt.time)
                        & (days_df['ts'].dt.time < end_alert_report.time())
                    ]
                )
                # Take the day of the alert and keep others
                alert_df = days_df[days_df['day'].dt.date == start_alert_report.date()]['p_cons']
                # Remove the day of the alert and keep others
                not_alert = days_df[days_df['day'].dt.date != start_alert_report.date()]['p_cons']
            # Sum for the alert and outside the alert
            sum_not_alert = not_alert.sum()
            sum_alert = alert_df.sum()
            # Mean during the alert and outside the alert
            mean_not_alert = not_alert.mean()
            mean_alert = alert_df.mean()
            # Compute global mean
            global_mean = (sum_alert + sum_not_alert) / (len(alert_df.index) + len(not_alert.index))
            # Register the percentage of reduction
            matrix[index_i][alert_idx + (i * (j + 1))] = (
                ((mean_alert - mean_not_alert) / global_mean)
                * 100
            )
            # Register the total sum of energy consumption in kWh
            sum_alerts[alert_idx + (i * (j + 1))] += (
                (sum_alert - (sum_not_alert * len(alert_df) / len(not_alert.index)))
                / (REPORTS_HOURS[j].total_seconds() / 3600)
            )


def non_contiguous_data(
    months_home: List[str],
    current_period: dt.datetime,
    sign: int
) -> Tuple[bool, str, dt.datetime]:
    """
    Temp function to find the following home where dates are not contiguous.

    :param months_home:     List of months for a home_id.
    :param current_period:  Current period
    :param sign:            The sign used for the delta. -1 if we need to check
                            before the current date, 1 otherwise.

    :return:                find, file_name, current_period.
    """
    find = False
    file_name = ""
    if sign == -1:
        if int(current_period.month) == 10:
            for i in range(len(months_home)):
                if months_home[i][12:14] == "08":
                    current_period += dt.timedelta(days=7 * 10 * sign)
                    find = True
                    file_name = months_home[i]
                    del months_home[i]
                    break
    elif sign == 1:
        if int(current_period.month) == 9:
            for i in range(len(months_home)):
                if months_home[i][12:14] == "11":
                    current_period += dt.timedelta(days=7 * 10 * sign)
                    find = True
                    file_name = months_home[i]
                    del months_home[i]
                    break
    return find, file_name, current_period


def compute_energy(series: pd.Series, dt: float) -> np.float64:
    """
    Function to compute the energy where the formula of the evergy is:
    E = dt * sum(Powers)

    :param series:  Series containing powers.
    :param dt:  Derivative of time expressed in hour.

    :return:    Return the Wh
    """
    return series.sum() * dt


def check_data() -> NoReturn:
    """
    Test for data to be sure that we have correct data.
    """
    # ECHL05 : 4 mai 2022
    df = pd.read_csv('resampled_data/ECH/ECHL05_2022_5_15min.csv')
    dates = [
        [dt.datetime(2022, 5, 4, 14, 00, 00), dt.datetime(2022, 5, 4, 17, 00, 00)],
        [dt.datetime(2022, 5, 11, 14, 00, 00), dt.datetime(2022, 5, 11, 17, 00, 00)],
        [dt.datetime(2022, 5, 18, 14, 00, 00), dt.datetime(2022, 5, 18, 17, 00, 00)],
        [dt.datetime(2022, 5, 25, 14, 00, 00), dt.datetime(2022, 5, 25, 17, 00, 00)]
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
