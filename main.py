from plot_load_curves import *
from reaction_sms import *
from utils import *
from config import *

#-----------------------------#
#----------MAIN CODE----------#
#-----------------------------#

"""
Function to manage data.
"""
def manageData():
    # For all communities
    for community in ["CDB"]:
        print("---------------Managing Data---------------")
        # For all file in the data folder
        for file in os.listdir(DATASET_FOLDER + '/' + community):
            print("---------------"+ file[:6] +"---------------")
            df = pd.read_csv(DATASET_FOLDER + '/' + community + '/' + file)

            # Check if there is a negative consumption
            if VERIFY_CONSUMPTION:
                checkNegativeConsumption(df, file[:6])

            # Check if we need to apply the correction
            if CONVERT_UTC_CET:
                utcToCet(df, file, community)

            # Run the resample function according to Resample boolean value
            if RESAMPLE:
                resampleDataset(file, df)

"""
Function for reactions.
"""
def computeAlertReaction():
    for community in COMMUNITY_NAME:
        print("--------------Computing Alerts--------------")
        path = RESAMPLED_FOLDER + '/' + community
        # For all file in the data folder
        for file in os.listdir(path):
            print("---------------"+ file[:6] +"---------------")
            df = pd.read_csv(path + '/' + file)
            # Find if a house reacted to the message
            if GLOBAL_REACTION:
                if community == "CDB":
                    findGlobalReaction(df, file, path, ALERTS_CDB, ALERT_REACTION_CDB, RANKING_ALERT_CDB)
                elif community == "ECH":
                    findGlobalReaction(df, file, path, ALERTS_ECH, ALERT_REACTION_ECH, RANKING_ALERT_ECH)
    
    print()
    print("----------CDB ALERTS RANKING----------")
    for key in RANKING_ALERT_CDB:
        print("The alert", key, f"{ALERTS_CDB[key]}", "obtained", RANKING_ALERT_CDB[key], "reactions.")
    print()
    print("----------CDB ALERTS RANKING----------")
    for key in ALERT_REACTION_CDB:
        print(f"The home id {key} reacted to the following alerts {ALERT_REACTION_CDB[key]}.")
        print(f"The home id {key} reacted to {len(ALERT_REACTION_CDB[key])/len(ALERTS_CDB)*100}% of the alerts.")


"""
Function to plot.
"""
def allPlots():
    # Choose the format
    fmt = '8S' if SEC8 else '15min'
    current_folder = DATASET_FOLDER if SEC8 else RESAMPLED_FOLDER
    # For all communities
    if BASIC_PLOT:
        for community in COMMUNITY_NAME:
            print("--------------Plotting--------------")
            # For all file in the data folder
            for file in os.listdir(current_folder + '/' + community):
                print("---------------"+ file[:6] +"---------------")
                df = pd.read_csv(current_folder + '/' + community + '/' + file)
                # Basic plot (15min or 8S (for 8sec))
                if SEC8:
                    home_id = df['home_id'].iloc[0]
                    path = f"plots/{community}/{home_id}"
                    plotBasicPeriod(df, path, home_id, "2022-05-23 00:00:00", "2022-05-29 23:59:52", fmt)
                else:
                    if (file[:3]  == "CDB" and int(file[12]) == 5 and int(file[12]) and int(file[7:11]) == 2022):
                        home_id = df['home_id'].iloc[0]
                        path = f"plots/{community}/{home_id}"
                        plotBasicPeriod(df, path, home_id, "2022-05-23 00:00:00", "2022-05-29 23:59:52", fmt)
    # Plot an average for a given date for a community
    if AVERAGE_COMMUNITY:
        print("--------------Plotting average--------------")
        plotAverageCommunity("2022-05-02 00:00:00", "2022-05-08 23:59:52", current_folder, 5, fmt)


"""
Main function
"""
def main():
 # Dictionaries containing if a participant respected the restriction period
    hours_mean_cdb = {}
    hours_mean_echap = {}
    count = 0
    
    # Manage the data (e.g. resample, etc.)
    if MANAGE_DATA:
        manageData()

    # Compute and show the information about the alert
    if REACTION:
        computeAlertReaction()

    # Plot 
    if PLOT:
        allPlots()


if __name__ == "__main__":
   main()