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
:param index:       Index of the home id.
"""
def findGlobalReaction(df, file_name, path, alerts, reaction, ranking, matrix, index):
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
                    elif int(file_name[4:6]) < int(file2[4:6]):
                        break
            # Compute the mean before and after the period of the alert
            mean = computeMeanUpToBound(df, copy.deepcopy(months_home), -1, alerts, starting_alert, ending_alert)
            mean += computeMeanUpToBound(df, copy.deepcopy(months_home), 1, alerts, starting_alert, ending_alert)
            mean /= 2
            
            mean_alert = df.query("ts >= \"" + alerts[i][0] + "\" and ts < \"" + alerts[i][1] + "\"")['p_cons'].mean()

            # We check if the mean is lower than during the alert
            if mean_alert < mean:
                ranking[i] = ranking[i] + 1 if i in ranking else 1
                if file_name[:6] in reaction:
                    reaction[file_name[:6]].append(i)
                else:
                    reaction[file_name[:6]] = [i]
            matrix[index][i] = ((mean_alert - mean) / mean) * 100 if mean != 0 else 0


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
        if current_period + delta not in alerts:
            # check if the month is the same
            if (current_period + delta).month == current_period.month:
                tmp_mean = tmp_df.query("ts >= \"" + str(current_period+delta) + "\" and ts < \"" 
                                    + str((current_period+delta) + delta_alert) + "\"")['p_cons'].mean()
                if tmp_mean > 0:
                    mean += tmp_mean
                    count += 1
                delta += datetime.timedelta(days=7*sign)
            else:
                find = False
                # Search the following month
                for i in range(len(months_home)):
                    if months_home[i][12] == (current_period + delta).month:
                        find = True
                        file_name = months_home[i]
                        del months_home[i]
                # If it is the case, we continue
                if find:
                    tmp_df = pd.read_csv(RESAMPLED_FOLDER + '/' + file_name[:3] + '/' + file_name)
                    tmp_mean = tmp_df.query("ts >= \"" + str(current_period+delta) + "\" and ts < \"" 
                                    + str((current_period+delta) + delta_alert) + "\"")['p_cons'].mean()
                    if tmp_mean > 0:
                        mean += tmp_mean
                        count += 1
                        delta += datetime.timedelta(days=7*sign)
                # Otherwise, there is no more file
                else:
                    finished = True
        else:
            delta += datetime.timedelta(days=7*sign)
    count = 1 if count == 0 else count
    return mean/count


"""
Function to find if there is a report of the consumption.
TODO: Adapt but after

:param df:  The dataframe.
"""
def findReport(df):
    global ALERTS_CDB, GLOBAL_MEAN, REPORTED
    home_id = df['home_id'].iloc[0]
    # For each alert
    reported = []
    for i in range(len(ALERTS_CDB)):
        # We check if a consumer reduced the consumption
        if GLOBAL_MEAN[home_id][i]:
            alert_period = datetime.datetime.strptime(ALERTS_CDB[i][1], '%Y-%m-%d %H:%M:%S')
            current_time = alert_period
            # We will check if the consumer reduced the consumption 
            while current_time < alert_period + datetime.timedelta(days=2):
                # Return the other days of the month
                # e.g. return all monday of the month
                date_to_check = findSameDaysInMonth(current_time)
                mean = 0
                nb_dates = len(date_to_check)
                alerted_df = df.query("ts >= \"" + str(current_time) + "\" and ts < \"" 
                            + str(current_time + datetime.timedelta(hours=1)) + "\"")
                # Average the consumption according the other day on the month
                for j in range(nb_dates):
                    tmp_df = df.query("ts >= \"" + str(date_to_check[j]) + "\" and ts < \"" 
                            + str(date_to_check[j] + datetime.timedelta(hours=1)) + "\"")
                    mean += (tmp_df["p_cons"].mean() / nb_dates)
                # The consumer reported the consumption
                if alerted_df["p_cons"].mean() > mean:
                    if current_time.day == alert_period.day:
                         reported.append("same day")
                    elif current_time.day - alert_period.day == 1:
                        reported.append("one day after")
                    else:
                        reported.append("two days after")
                    break
                else:
                    # Increment
                    current_time += datetime.timedelta(hours=1)

        # The consumer did'nt report the consumption
        if home_id not in REPORTED:
            reported.append("never")
    REPORTED[home_id] = reported


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
