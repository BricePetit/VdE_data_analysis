import pandas as pd
import numpy as np
# import scipy
import matplotlib.pyplot as plt
import os
import datetime


#------------------------------------#
#----------GLOBAL VARIABLES----------#
#------------------------------------#


# Name of the folder where the dataset is located
DATASET_FOLDER = 'dataset'

# Name of the folder where the resampled dataset is located
RESAMPLED_FOLDER = 'resampled_data'

# True if we want to resample the dataset
RESAMPLE = False

# True if we want to verify if there are negative consumptions
VERIFY_CONSUMPTION = False

# Number of minutes that is used during the resampled dataset
MINUTES = 15

# Maybe useless => remove if it is
MONTHS = {'January': 31,'February':28,'March':31,'April':30,'May':31,'June':30,'July':31,'August':31,
            'September':30, 'October':31,'November':30,'December':31}

# List of period where the consumer need to reduce the consumption
ALERTS_CDB = [["2022-05-04 14:00:00", "2022-05-04 16:00:00"], 
              ["2022-05-15 17:00:00", "2022-05-15 19:00:00"]]

# Dictionaries containing if a participant respected the restriction period from a global 
# point of view
GLOBAL_MEAN = {}
GLOBAL_MEDIAN = {}

# Compute the number of hours in the alert period
HOURS_ALERT_PERIOD = 0
for msg in ALERTS_CDB:
    lower_bound = datetime.datetime.strptime(msg[0], '%Y-%m-%d %H:%M:%S')
    upper_bound = datetime.datetime.strptime(msg[1], '%Y-%m-%d %H:%M:%S')
    if HOURS_ALERT_PERIOD < int(((upper_bound - lower_bound).total_seconds())/3600):
        HOURS_ALERT_PERIOD = int(((upper_bound - lower_bound).total_seconds())/3600)

# Dictionaries containing if a participant respected the restriction period from hours 
# point of view
HOURS_MEDIAN = {}
HOURS_MEAN = {}

# Dictionary to know if the consumption has been reported
REPORTED = {}


#----------------------------------#
#----------MAIN FUNCTIONS----------#
#----------------------------------#


"""
Check if there is negative consumption in the dataframe. It will print the home_id and indexes 
of the dataframe.
"""
def checkNegativeConsumption(df, home_id):
    if (df['p_cons'] < 0).any().any():
        print()
        print("/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\")
        print("Home: " + home_id)
        print(df.index[df['p_cons'] < 0])
        print("/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\/!\\")
        print()


"""
Function to resample the dataset. In order to apply that, we will apply an average of value 
according to the number of minutes. When it's done, we write the 

:param file:    Name of the file.
:param df:      Dataframe.
:param days:    Number of days in the month.
"""
def resampleDataset(file, df, days=31):
    global MINUTES
    # Create a new data frame as the original
    new_df = pd.DataFrame(columns=['home_id','day','ts','p_cons','p_prod','p_tot'])
    initial_time = datetime.datetime.strptime(df['ts'][0], '%Y-%m-%d %H:%M:%S')
    # It represents the duration for the average
    delta = datetime.timedelta(minutes=MINUTES)
    current_ts = initial_time
    # Loop until the end of the month
    while current_ts < initial_time + datetime.timedelta(days=days):
        # Apply the query where we want: current_ts <= ts < current_ts + delta
        tmp_df = df.query("ts >= \"" + str(current_ts) + "\" and ts < \"" + str(current_ts + delta)  + "\"")
        home_id = tmp_df['home_id'].iloc[0]
        day = tmp_df['day'].iloc[0]
        # Apply the average for the consumption values
        average_p_cons = round(tmp_df['p_cons'].mean(),0)
        average_p_prod = round(tmp_df['p_prod'].mean(),0)
        average_p_tot = round(tmp_df['p_tot'].mean(),0)
        # Create a temporary Dataframe with new values to combine with the final Dataframe
        tmp_df2 = pd.DataFrame([[home_id,day,str(current_ts),average_p_cons,average_p_prod,average_p_tot]], columns=['home_id','day','ts','p_cons','p_prod','p_tot'])
        new_df = pd.concat([new_df, tmp_df2], ignore_index=True)
        # Update the timestamp
        current_ts = current_ts + delta
    # Write the sampled dataset
    new_df.to_csv(RESAMPLED_FOLDER + '/' + file[:-4] + '_' + str(MINUTES) +'min.csv', index=False)


"""
Function to find a reaction in the dataframe in a global point of view.

:param df: The dataframe.
"""
def findGlobalReaction(df):
    global GLOBAL_MEDIAN, GLOBAL_MEAN, MINUTES
    global_reacted_msg_median = []
    global_reacted_msg_mean = []
    home_id = df['home_id'].iloc[0]

    for msg in ALERTS_CDB:
        # Take the data during the alerted period
        alerted_period = df.query("ts >= \"" + msg[0] + "\" and ts < \"" + msg[1] + "\"")
        # Take the first timestamp of the period and create a datetime
        initial_period = datetime.datetime.strptime(msg[0], '%Y-%m-%d %H:%M:%S')
        # Take the value before the period
        before_period = df.query("ts == \"" + str(initial_period - datetime.timedelta(minutes=MINUTES)) + "\"")
        # Take the consumption value before the period
        p_cons_before_period = before_period["p_cons"].iloc[0]
        # Compute the median and add it into a list
        median = alerted_period["p_cons"].median()
        global_reacted_msg_median.append(1 if p_cons_before_period > median else 0)
        # Compute the mean and add it into a list
        mean = alerted_period["p_cons"].mean()
        global_reacted_msg_mean.append(1 if p_cons_before_period > mean else 0)
        # Print useful information
        print(msg)
        print()
        print("Consumption before the period:" + str(p_cons_before_period))
        print("Median consumption during the period: " + str(median))
        print("Mean consumption during the period: " + str(mean) + "\n")
        # print("Standard deviation: " + str(alerted_period["p_cons"].std())) #  + "\n"
        # print(scipy.stats.zscore(alerted_period["p_cons"]))
        print("All values:")
        print(alerted_period["p_cons"])
        print()

    # Add values in a dictionary according to the home_id
    GLOBAL_MEDIAN[home_id] = global_reacted_msg_median
    GLOBAL_MEAN[home_id] = global_reacted_msg_mean

"""
Function to find a reaction in the dataframe in hours point of view.

:param df: The dataframe.
"""
def findHourReaction(df):
    global HOURS_MEDIAN, HOURS_MEAN, HOURS_ALERT_PERIOD, MINUTES
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


"""


Weekday:
    - Monday == 0
    - Tuesday == 1
    - Wednesday == 2
    - Thursday == 3
    - Friday == 4
    - Saturday == 5
    - Sunday == 6

:param df:  Dataframe
"""
def loadCurve(df):
    days = 7
    average_p_cons = []
    initial_period = datetime.datetime.strptime(df['ts'].iloc[0], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=1)
    
    week = df.query("ts >= \"" + str(initial_period) + "\" and ts < \""  
            + str(initial_period + datetime.timedelta(days=days - initial_period.weekday())) + "\"")
    week = week.groupby(['day'])
    average_p_cons.append(week['p_cons'].mean())
    average_p_cons.append(week['p_cons'].mean())
    # Change the index of the pandas series in order to obtain a more readable graph
    index = []
    for idx in average_p_cons[0].index:
        index.append(datetime.datetime.strptime(idx, '%Y-%m-%d').day)
    average_p_cons[0].set_axis(index, inplace=True)


    # voir pour plot tous les jours de la semaine
    plt.rcParams["figure.figsize"] = (7,7)
    plt.rcParams["figure.autolayout"] = True
    fig, axes = plt.subplots(nrows=2, ncols=2)
    list_size = int(len(average_p_cons)/2)
    week_nb = 1
    for i in range(list_size):
        for j in range(list_size+1):
            average_p_cons[i].plot(x='ts', ax=axes[i,j], title=f'Week {week_nb}', ylim=0)
            # axes[i,j].set_xticklabels(index)
            axes[i,j].tick_params
            week_nb += 1
    fig.suptitle(f'Load curve for {initial_period.strftime("%B")}')

    plt.show()
    plt.close(fig)
    # day = df['day'].iloc[0]
    # tmp = pd.DataFrame([[day,str(current_ts),average_p_cons]], columns=['day','ts','p_cons'])
    # daily = pd.concat([daily, tmp], ignore_index=True)



"""
Plot each wednesday
"""
def plotWednesday(df):
    global ALERTS_CDB
    plt.rcParams["figure.figsize"] = (10,10)
    WEDNESDAY = [["2022-05-11 00:00:00","2022-05-11 23:59:52"],
            ["2022-05-18 00:00:00","2022-05-18 23:59:52"],
            ["2022-05-25 00:00:00","2022-05-25 23:59:52"],
            ["2022-05-04 00:00:00","2022-05-04 23:59:52"]]
    # alerted = df.query("ts >= \"" + ALERTS_CDB[0][0] + "\" and ts <= \"" + ALERTS_CDB[0][1] + "\"")
    normal_wednesday = []
    for i in range(len(WEDNESDAY)):
        normal_wednesday.append(df.query("ts >= \"" + WEDNESDAY[i][0] + "\" and ts <= \"" + WEDNESDAY[i][1] + "\""))
    fig, axes = plt.subplots(nrows=2, ncols=2)
    # alerted.iloc[:,[2,3]].plot(x='ts', ax=axes[0,0], rot=10, title='2022-05-04', ylim=0)
    normal_wednesday[3].iloc[:,[2,3]].plot(x='ts', ax=axes[0,0], rot=10, title='2022-05-04', ylim=0)
    normal_wednesday[0].iloc[:,[2,3]].plot(x='ts', ax=axes[0,1], rot=10, title='2022-05-11', ylim=0)
    normal_wednesday[1].iloc[:,[2,3]].plot(x='ts', ax=axes[1,0], rot=10, title='2022-05-18', ylim=0)
    normal_wednesday[2].iloc[:,[2,3]].plot(x='ts', ax=axes[1,1], rot=10, title='2022-05-25', ylim=0)

    fig.savefig(f'plots/{file[:6]}Wednesday.png')
    plt.close(fig)


#-----------------------------#
#----------MAIN CODE----------#
#-----------------------------#


if __name__ == "__main__":
    # For all file in the data folder
    count = 0
    for file in os.listdir(RESAMPLED_FOLDER):
        print("---------------"+ file[:6] +"---------------")
        df = pd.read_csv(RESAMPLED_FOLDER + '/' + file)

        # Check if there is a negative consumption
        if VERIFY_CONSUMPTION:
            checkNegativeConsumption(df, file[:6])

        # Run the resample function according to Resample boolean value
        if RESAMPLE:
            resampleDataset(file, df)

        # If the filename is not in the list due to some errors in the configuration
        # if file[:6] not in ['CDB004', 'CDB012', 'CDB028']:
        #     print("--------Global point of view--------\n")
        #     findGlobalReaction(df)
        #     print("--------Hours point of view---------\n")
        #     findHourReaction(df)
        #     if GLOBAL_MEAN[file[:6]][0] or GLOBAL_MEAN[file[:6]][1]:
        #         findReport(df)
        #     count += 1
        #     plotWednesday(df)
    # loadCurve(df)
    # Compute the percentage of people that reacted to the message
    # computeStats()
