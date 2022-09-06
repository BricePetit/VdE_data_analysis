__title__ = "utils"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


from config import *

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
Function to check inconsistencies. There are 5 cases:
    - Case 1: Missing data during a period. (p_cons == 0)
    - Case 2: p_cons == p_tot && p_prod == 0 && p_cons neg
    - Case 3: p_cons neg (so inverted)
    - Case 4: inconsistent data spike (p_cons <= -100000 && p_prod >= 100000 ||
                                        p_cons >= 100000 && p_prod <= -100000)
    - Case 5: p_cons == -p_prod => p_tot == 0 (p_cons >= 0 && p_prod <= 0)
"""
def checkMistake():
    error_msg = "#----------Kind of problems----------#\n"
    error_msg += "#Case 1: Missing data during a period. (p_cons == 0)\n"
    error_msg += "#Case 2: p_cons == p_tot && p_prod == 0 && p_cons neg\n"
    error_msg += "#Case 3: p_cons neg (so inverted)\n"
    error_msg += "#Case 4: inconsistent data spike (p_cons <= -100000 && p_prod >= 100000 || p_cons >= 100000 && p_prod <= -100000)\n"
    error_msg += "#Case 5: p_cons == -p_prod => p_tot == 0 (p_cons >= 0 && p_prod <= 0)\n"
    for community in COMMUNITY_NAME:
        print("---------------Checking Inconsistencies---------------")
        # For all file in the data folder
        for file in os.listdir(RESAMPLED_FOLDER + '/' + community):
            print("#----------" + file + "----------#")
            error_msg += "#----------" + file + "----------#\n"
            df = pd.read_csv(RESAMPLED_FOLDER + '/' + community + '/' + file)
            for i in range(len(df.index)):
                # Case 1
                if df['p_cons'].iloc[i] == 0:
                    error_msg += "Case 1: " + df['ts'].iloc[i] + "\n"
                # Case 2, 3, 4
                elif df['p_cons'].iloc[i] < 0:
                    # Case 2
                    if df['p_cons'].iloc[i] == df['p_tot'].iloc[i] and df['p_prod'].iloc[i] == 0:
                        error_msg += "Case 2: " + df['ts'].iloc[i] + "\n"
                    # Case 4
                    elif df['p_cons'].iloc[i] <= -100000 and df['p_prod'].iloc[i] >= -100000:
                        error_msg += "Case 4: " + df['ts'].iloc[i] + "\n"
                    # Case 3
                    else:
                        error_msg += "Case 3: " + df['ts'].iloc[i] + "\n"
                # Case 4
                elif df['p_cons'].iloc[i] >= -100000 and df['p_prod'].iloc[i] <= -100000:
                    error_msg += "Case 4: " + df['ts'].iloc[i] + "\n"
                # Case 5
                elif df['p_cons'].iloc[i] == (df['p_prod'].iloc[i] * -1) and df['p_tot'].iloc[i] == 0:
                    if df['p_cons'].iloc[i] > 0 and df['p_prod'].iloc[i] < 0:
                        error_msg += "Case 5: " + df['ts'].iloc[i] + "\n"
    # Write the message
    f = open("all_problems.txt", 'w')
    f.write(error_msg)
    f.close()


"""
Function to resample the dataset. In order to apply that, we will apply an average of values 
according to the number of minutes. When it's done, we write the dataframe in a file according to 
the month.

:param file:    Name of the file.
:param df:      Dataframe.
"""
def resampleDataset(file, df):
    # Create a new data frame as the original
    new_df = pd.DataFrame(columns=['home_id','day','ts','p_cons','p_prod','p_tot'])
    initial_time = datetime.datetime.strptime(df['ts'][0], '%Y-%m-%d %H:%M:%S')
    # It represents the duration for the average
    delta = datetime.timedelta(minutes=15)
    
    # We adjust the time in order to start at 00, 15, 30 or 45
    if initial_time.second % 60 != 00:
        initial_time += datetime.timedelta(seconds=(60 - (initial_time.second % 60)))
        if initial_time.minute % 15 != 00:
            initial_time += datetime.timedelta(minutes=(15 - (initial_time.minute % 15)))
    current_ts = initial_time
    # Loop until the end of the month
    last_ts = datetime.datetime.strptime(df['ts'].iloc[-1], '%Y-%m-%d %H:%M:%S')
    while current_ts < last_ts:
        # Apply the query where we want: current_ts <= ts < current_ts + delta
        tmp_df = df.query("ts >= \"" + str(current_ts) + "\" and ts < \"" + str(current_ts + delta)  + "\"")
        # Check if the df is not empty because it is possible to obtain a gap between periods
        if  tmp_df.size != 0:
            home_id = tmp_df['home_id'].iloc[0]
            day = tmp_df['day'].iloc[0]
            # Apply the average for the consumption values
            average_p_cons = round(tmp_df['p_cons'].mean(),0)
            average_p_prod = round(tmp_df['p_prod'].mean(),0)
            average_p_tot = round(tmp_df['p_tot'].mean(),0)
            # Create a temporary Dataframe with new values to combine with the final Dataframe
            tmp_df2 = pd.DataFrame([[home_id,day,current_ts,average_p_cons,average_p_prod,average_p_tot]], 
                                    columns=['home_id','day','ts','p_cons','p_prod','p_tot'])
            new_df = pd.concat([new_df, tmp_df2], ignore_index=True)
            # If the next 15 minutes are in another month, we will save the current month.
            if current_ts.month != (current_ts + delta).month:
                new_df.to_csv(RESAMPLED_FOLDER + '/' + file[:3] + '/' + file[:-4] + '_' 
                                + str(current_ts.year) + "-" + str(current_ts.month) 
                                + '_15min.csv', index=False)
                new_df = pd.DataFrame(columns=['home_id','day','ts','p_cons','p_prod','p_tot'])
        # Update the timestamp
        current_ts = current_ts + delta
    # Write the sampled dataset
    new_df.to_csv(RESAMPLED_FOLDER + '/' + file[:3] + '/' + file[:-4] + '_' + str(current_ts.year) + "-" 
                    + str(current_ts.month) + '_15min.csv', index=False)


"""
Basically, the database encodes TS in UTC. So, we add 2 hours to the ts to obtain the correct TS.
/!\ We need to check before if the data are in UTC or CET. /!\ 
/!\ If it is in UTC, there are 2 different values => e.g. 2022-05-02 & 2022-05-03 22:00:00 /!\

:param df:              Dataframe
:param file_name:       Name of the file & the extension
:param community_name:  Name of the community
"""
def utcToCet(df, file_name, community_name):
    # Convert the time
    time_check = datetime.datetime.strptime(df['ts'].iloc[0], '%Y-%m-%d %H:%M:%S')
    time_check = time_check.strftime('%Y-%m-%d') + " " + "23:00:00"
    # Query the dataframe
    time_df = df.query("ts == \"" + time_check + "\"")
    t1 = datetime.datetime.strptime(time_df['ts'].iloc[0] , '%Y-%m-%d %H:%M:%S')
    t2 = datetime.datetime.strptime(time_df['day'].iloc[0] , '%Y-%m-%d')
    # If months are different => not in CET.
    if t1.day != t2.day:
        for i in range(len(df)):
            # Apply the conversion of the timestamp
            from_zone = tz.gettz('UTC')
            to_zone = tz.gettz("Europe/Brussels")
            utc = datetime.datetime.strptime(df.loc[i, 'ts'], '%Y-%m-%d %H:%M:%S')
            utc = utc.replace(tzinfo=from_zone)
            utc = utc.astimezone(to_zone)
            df.loc[i, 'ts'] = utc.replace(tzinfo=None)
        df.to_csv(DATASET_FOLDER + '/'+ community_name + '/' + file_name, index=False)
        print("Done!")
    else:
        print("Already correct!")

"""
Export the dataframe in a excel file.

:param matrix:      Matrix containing the results of alerts.
:param home_ids:    The id of each home.
:param alerts:      Period of alerts.
:param sum_alerts:  List of all consumption during alerts - same period outside alerts.
:param path:        The path to register the excel file.
"""
def exportToXLSX(matrix, home_ids, alerts, sum_alerts, file_name):
    alerts = ['A'+ str(i+1) for i in range(len(alerts))]
    df = pd.DataFrame(data=np.array(matrix), index=home_ids, columns=alerts)
    tmp_df = pd.DataFrame(data=[sum_alerts], index=["Bilan"], columns=alerts)
    df = pd.concat([df, tmp_df])
    print(df)
    df.to_excel(excel_writer = file_name)
