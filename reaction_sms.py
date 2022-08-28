from config import *

#--------------------------------------#
#----------REACTION FUNCTIONS----------#
#--------------------------------------#


"""
Function to find a reaction in the dataframe in a global point of view.

:param df: The dataframe.
"""
def findGlobalReaction(df, file_name, path, alerts):
    for msg in alerts:
        alert_period = datetime.datetime.strptime(msg[0], '%Y-%m-%d %H:%M:%S')
        if int(file_name[7:11]) == alert_period.year and int(file_name[12]) == alert_period.month:
            months_home = []
            # We recover all month for the current home
            for file2 in os.listdir(path):
                if file_name != file2:
                    if file_name[:6] == file2[:6]:
                        months_home.append(file2)
                    elif int(file_name[4:6]) < int(file2[4:6]):
                        break
            min_period = datetime.datetime(day=1, month=int(months_home[0][12]), year=int(months_home[0][7:11]))
            last_day = calendar.monthrange(int(months_home[-1][7:11]), int(months_home[-1][12]))[1];
            max_period = datetime.datetime(day=last_day, month=int(months_home[-1][12]), year=int(months_home[-1][7:11]))

            if alert_period.isoweekday() == min_period.isoweekday():
                pass
                

"""
TODO: Transform in a global approach

:param df:          Dataframe
:param alerts:      List of alerts
:hours_mean:        Dictionaries containing if a participant respected the restriction period.
:param minutes:     The number of minutes before the alert.
:param community:   Name of the community (CDB or ECH).

:return:            Return hours_mean that is the dictionary that contains the participation of 
                    consumers.
"""
def findHourReaction(df, alerts, hours_mean, minutes, community):
    hour_reacted_msg_mean = []
    home_id = df['home_id'].iloc[0]

    for msg in alerts:
        # Take the first timestamp of the period and create a datetime
        initial_period = datetime.datetime.strptime(msg[0], '%Y-%m-%d %H:%M:%S')

        date = datetime.datetime.strptime(msg[0], '%Y-%m-%d')
        # Take the value before the period
        if date < datetime.datetime.strptime('2022-07-01', '%Y-%m-%d'):
            # Case where we are before July in the Coin du Balai community
            if community == "CDB" and str(date) in ["2022-04-29", "2022-05-15", "2022-06-11"]:
                # Find another initial period knowing that they have the msg 
                # They have always known the information 
                count = 7
                tmp_p_cons = 0
                for _ in range(4):
                    if date not in ["2022-04-29", "2022-05-15", "2022-06-11"]:
                        before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(day=count)) + "\"")
                        tmp_p_cons += before_period["p_cons"].iloc[0]
                    else:
                        count += 7
                        before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(day=count)) + "\"")
                        tmp_p_cons += before_period["p_cons"].iloc[0]
                    count += 7
                # Take the average consumption value before the period. The average is done on each week with the same day
                p_cons_before_period = round(tmp_p_cons / 4,0)
            # Case where we are before July in the Echappée community
            else:
                before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(minutes=45)) + "\"")
                # Take the consumption value before the period
                p_cons_before_period = before_period["p_cons"].iloc[0]
        else:
            # Case where we are in July or later in the Coin du Balai community
            if community == "CDB":
                # Need to check the 
                before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(day=minutes)) + "\"")
            # Case where we are in July or later in the Echappée community
            else:
                # We choose 135 because the experimentation is 120 min before and we did an average, so 
                # we take 15 min before
                before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(minutes=135)) + "\"")
            p_cons_before_period = before_period["p_cons"].iloc[0]

        # Take the last timestamp of the period and create a datetime
        last_period = datetime.datetime.strptime(msg[1], '%Y-%m-%d %H:%M:%S')
        current_time = initial_period
        print(msg)
        print()
        while current_time < last_period:
            # Take the df per hour
            df_per_hour = df.query("ts >= \"" + str(current_time) + "\" and ts < \"" 
                        + str(current_time + datetime.timedelta(minutes=60)) + "\"")
            # Compute the mean and add it into a list
            mean = df_per_hour["p_cons"].mean()
            hour_reacted_msg_mean.append(1 if p_cons_before_period > mean else 0)

            print(current_time)
            print()
            print("Consumption before the period:" + str(p_cons_before_period))
            print("Mean consumption during the period: " + str(mean) + "\n")
            # print("Standard deviation: " + str(df_per_hour["p_cons"].std()) + "\n")
            print("All values:")
            print(df_per_hour["p_cons"])
            print()

            current_time += datetime.timedelta(minutes=60)

    hours_mean[home_id] = hour_reacted_msg_mean

    return hours_mean

"""
Function to find a reaction in the dataframe in hours point of view.

:param df: The dataframe.
"""
def findHourReaction(df):
    global HOURS_MEDIAN, HOURS_MEAN, HOURS_ALERT_PERIOD, MINUTES, ALERTS_CDB
    hour_reacted_msg_median = []
    hour_reacted_msg_mean = []
    home_id = df['home_id'].iloc[0]

    for msg in ALERTS_CDB:
        # Take the first timestamp of the period and create a datetime
        initial_period = datetime.datetime.strptime(msg[0], '%Y-%m-%d %H:%M:%S')
        # Take the value before the period
        before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(minutes=MINUTES)) + "\"")
        # Take the consumption value before the period
        p_cons_before_period = before_period["p_cons"].iloc[0]
        # Take the last timestamp of the period and create a datetime
        last_period = datetime.datetime.strptime(msg[1], '%Y-%m-%d %H:%M:%S')
        current_time = initial_period
        count = 0
        print(msg)
        print()
        while current_time < last_period:
            # Take the df per hour
            df_per_hour = df.query("ts >= \"" + str(current_time) + "\" and ts < \"" 
                        + str(current_time + datetime.timedelta(minutes=60)) + "\"")
            # Compute the median and add it into a list
            median = df_per_hour["p_cons"].median()
            hour_reacted_msg_median.append(1 if p_cons_before_period > median else 0)
            # Compute the mean and add it into a list
            mean = df_per_hour["p_cons"].mean()
            hour_reacted_msg_mean.append(1 if p_cons_before_period > mean else 0)

            print(current_time)
            print()
            print("Consumption before the period:" + str(p_cons_before_period))
            print("Median consumption during the period: " + str(median))
            print("Mean consumption during the period: " + str(mean) + "\n")
            # print("Standard deviation: " + str(df_per_hour["p_cons"].std()) + "\n")
            print("All values:")
            print(df_per_hour["p_cons"])
            print()

            count += 1
            current_time += datetime.timedelta(minutes=60)

        # Check if the size are identical
        if count < HOURS_ALERT_PERIOD:
            for _ in range(HOURS_ALERT_PERIOD-count):
                hour_reacted_msg_median.append(None)
                hour_reacted_msg_mean.append(None)

    HOURS_MEDIAN[home_id] = hour_reacted_msg_median
    HOURS_MEAN[home_id] = hour_reacted_msg_mean


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
