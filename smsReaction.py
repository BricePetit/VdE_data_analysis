__title__ = "smsReaction"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


from sys import set_coroutine_origin_tracking_depth
from config import *

#--------------------------------------#
#----------REACTION FUNCTIONS----------#
#--------------------------------------#


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
def findGlobalReactionAndReport(df, file_name, path, alerts, reaction, ranking, matrix, sum_alerts, index):
    for i in range(len(alerts)):
        starting_alert = datetime.datetime.strptime(alerts[i][0], '%Y-%m-%d %H:%M:%S')
        ending_alert = datetime.datetime.strptime(alerts[i][1], '%Y-%m-%d %H:%M:%S')
        if int(file_name[7:11]) == starting_alert.year and int(file_name[12]) == starting_alert.month:
            months_home = []
            # We recover all month for the current home
            for file2 in os.listdir(path):
                if file_name != file2:
                    if file_name[:6] == file2[:6]:
                        months_home.append(file2)
            # Compute the mean before and after the period of the alert
            mean = computeMeanUpToBound(df, copy.deepcopy(months_home), -1, alerts, starting_alert, ending_alert)
            mean += computeMeanUpToBound(df, copy.deepcopy(months_home), 1, alerts, starting_alert, ending_alert)
            # Mean during the alert        
            mean_alert = df.query("ts >= \"" + alerts[i][0] + "\" and ts < \"" + alerts[i][1] + "\"")['p_cons'].mean()
            # Global mean including the alert. We divide by 3 because we add 3 values
            mean = (mean + mean_alert) / 3
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
            matrix[index][alert_idx] = ((mean_alert - mean) / mean) * 100 if mean != 0 else 0
            # Compute the sum 
            sum_alerts[alert_idx] += mean_alert - mean
            # Find report
            findReport(df, alerts, months_home,  matrix, sum_alerts, index, i)


"""
This function will compute the mean before or after the alert according to the sign

:param df:              Dataframe
:param months_home:     List of months for a home_id
:param sign:            The sign used for the delta. -1 if we need to check 
                        before the current date, 1 otherwise
:param alerts:          All alerts
:param starting_alert:  The beginning of the alert.
:param ending_alert:    The end of the alert.

:return:                Return the mean.
"""
def computeMeanUpToBound(df, months_home, sign, alerts, starting_alert, ending_alert):
    finished = False
    current_period = starting_alert
    # The time between the beginning of the alert and the end of the alert
    delta_alert = ending_alert - starting_alert
    delta = datetime.timedelta(days=7*sign)
    mean = 0
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
            if current_period.month == (current_period-delta).month:
                tmp_mean = tmp_df.query("ts >= \"" + str(current_period) + "\" and ts < \"" 
                                    + str(current_period + delta_alert) + "\"")['p_cons'].mean()
                # If the consumption is not missing or negative
                if tmp_mean > 0:
                    mean += tmp_mean
                    count += 1
            else:
                find = False
                # Search the following month
                for i in range(len(months_home)):
                    # Check if years are equal
                    if int(months_home[i][7:11]) == int(current_period.year):
                        # Check if months are equals, case where the month is one or two digit(s)
                        if int(months_home[i][12]) == int(current_period.month) or months_home[i][12:14] == str(current_period.month):
                            find = True
                            file_name = months_home[i]
                            del months_home[i]
                            break
                # If it is the case, we continue
                if find:
                    tmp_df = pd.read_csv(RESAMPLED_FOLDER + '/' + file_name[:3] + '/' + file_name)
                    tmp_mean = tmp_df.query("ts >= \"" + str(current_period) + "\" and ts < \"" 
                                    + str(current_period + delta_alert) + "\"")['p_cons'].mean()
                    # If the consumption is not missing or negative
                    if tmp_mean > 0:
                        mean += tmp_mean
                        count += 1
                # Otherwise, there is no more file
                else:
                    finished = True
        # should be >= now
        if current_period + delta >= datetime.datetime.strptime("2022-08-12 15:45:00", '%Y-%m-%d %H:%M:%S'):
            finished = True
        else:
            current_period += delta
    return mean/(1 if count == 0 else count)


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
def findReport(df, alerts, months_home, matrix, sum_alerts, index_i, index_j):
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
                starting_alert = datetime.datetime.strptime(alerts[index_j][0], '%Y-%m-%d %H:%M:%S') + (-1 * REPORTS_HOURS[j])
                ending_alert = datetime.datetime.strptime(alerts[index_j][0], '%Y-%m-%d %H:%M:%S')
            else:
                starting_alert = datetime.datetime.strptime(alerts[index_j][1], '%Y-%m-%d %H:%M:%S')
                ending_alert = datetime.datetime.strptime(alerts[index_j][1], '%Y-%m-%d %H:%M:%S') + (REPORTS_HOURS[j])
            # Compute the mean before and after the period of the alert
            mean = computeMeanUpToBound(df, copy.deepcopy(months_home), -1, alerts, starting_alert, ending_alert)
            mean += computeMeanUpToBound(df, copy.deepcopy(months_home), 1, alerts, starting_alert, ending_alert)
            # Mean during the alert        
            mean_alert = df.query("ts >= \"" + str(starting_alert) + "\" and ts < \"" + str(ending_alert) + "\"")['p_cons'].mean()
            # Global mean including the alert. We divide by 3 because we add 3 values
            mean = (mean + mean_alert) / 3
            matrix[index_i][alert_idx + (i * (j + 1))] = ((mean_alert - mean) / mean) * 100 if mean != 0 else 0
            sum_alerts[alert_idx + (i * (j + 1))] += mean_alert - mean


"""
Function to find all days that identical in the month.
e.g. with the date 2022-05-04, we want to find 2022-05-11, 2022-05-18 and 2022-05-25

:param current_date:    The current date.

:return:                Return the list of all dates.
"""
def findSameDaysInMonth(current_date):
    date_to_check = []
    backward = True
    forward = True
    delta = datetime.timedelta(days=7)
    while backward or forward:
        if backward:
            # Check all days before the current date
            if (current_date - delta).month == current_date.month:
                date_to_check.append(current_date - delta)
                delta += datetime.timedelta(days=7)
            else:
                backward = False
                delta = datetime.timedelta(days=7)
        else:
            # Check all days after the current date
            if (current_date + delta).month == current_date.month:
                date_to_check.append(current_date + delta)
                delta += datetime.timedelta(days=7)
            else:
                forward = False
                delta = datetime.timedelta(days=7)
    return date_to_check


"""
Evaluate the number of people that reduced the consumption according to the message and 
print results.
"""
def computeStats():
    global ALERTS_CDB, HOURS_ALERT_PERIOD, GLOBAL_MEDIAN, GLOBAL_MEAN, HOURS_MEDIAN, HOURS_MEAN, REPORTED
    global_median_reaction = np.zeros(len(ALERTS_CDB))
    global_mean_reaction = np.zeros(len(ALERTS_CDB))

    hours_median_reaction = np.zeros(HOURS_ALERT_PERIOD*2)
    hours_mean_reaction = np.zeros(HOURS_ALERT_PERIOD*2)

    # Compute the percentage of people that reacted to the message
    # We check a global reaction and a reaction per hour
    for key in GLOBAL_MEDIAN:
        for i in range(len(ALERTS_CDB)):
            global_median_reaction[i] += (GLOBAL_MEDIAN[key][i] / count * 100)
            global_mean_reaction[i] += (GLOBAL_MEAN[key][i] / count * 100)
        for i in range(HOURS_ALERT_PERIOD*2):
            hours_median_reaction[i] += (HOURS_MEDIAN[key][i] / count * 100)
            hours_mean_reaction[i] += (HOURS_MEAN[key][i] / count * 100)

    print("Number of houses:", str(count))
    print()
    for i in range(len(ALERTS_CDB)):
        print("Message ", i + 1, ": ", ALERTS_CDB[i])
    print()
    print("----------Global point of view----------\n")
    for i in range(len(ALERTS_CDB)):
        print("Median for the message " + str(i + 1) + ": " + str(round(global_median_reaction[i], 2)) + "%")
        print("Mean for the message " + str(i + 1) + ": " + str(round(global_mean_reaction[i], 2)) + "%")
        print()
    print("-----------Hours point of view----------\n")
    for i in range(len(ALERTS_CDB)):
        for j in range(HOURS_ALERT_PERIOD):
            print("Median for the message " + str(i + 1) + " hour " + str(j + 1) + ": " 
                    + str(round(hours_median_reaction[i * HOURS_ALERT_PERIOD + j], 2)) + "%")
            print("Mean for the message " + str(i + 1) + " hour " + str(j + 1) + ": " 
                    + str(round(hours_mean_reaction[i * HOURS_ALERT_PERIOD + j], 2)) + "%")
        print()

    for i in range(len(ALERTS_CDB)):
        count_reported = 0
        print("Message ", i + 1, ": ", ALERTS_CDB[i])
        print()
        for key in REPORTED:
            if REPORTED[key][i] != "never":
                print(f"The house {key} has reported the consumption {REPORTED[key][i]}.")
                count_reported += 1
            else:
                print(f"The house {key} has {REPORTED[key][i]} reported the consumption.")
        print()
        print(f"{round(count_reported/len(REPORTED)*100, 2)}% of consumers reported the consumptions.")
        print()


"""
Function to count the number of consumer that reacted to an alert.

:return:    Return a dictionary with the number of people that reacted to an alert.
            The key is 0, 1, 2, etc. Where it represents the index of the alert.
"""
def rankAlerts():
    global GLOBAL_MEAN, ALERTS_CDB
    alerts_ranking = {}
    # For each alerts
    for i in range(len(ALERTS_CDB)):
        # For each person 
        for key in GLOBAL_MEAN:
            if GLOBAL_MEAN[key][i] == 1:
                # Ternary operator to check if the index is already in the dictionary
                # If it is the case, we add 1 else we create the key
                alerts_ranking[i] = alerts_ranking[i] + 1 if i in alerts_ranking else 1
    return alerts_ranking