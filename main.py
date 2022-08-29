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


"""
Function to plot.
"""
def allPlots():
    # For all communities
    for community in COMMUNITY_NAME:
        print("--------------Plotting--------------")
        # For all file in the data folder
        for file in os.listdir(RESAMPLED_FOLDER + '/' + community):
            print("---------------"+ file[:6] +"---------------")
            df = pd.read_csv(RESAMPLED_FOLDER + '/' + community + '/' + file)
            # Basic plot (15min or 8S (for 8sec))
            if BASIC_PLOT:
                if (file[:4]  == "ECHL" and int(file[12]) == 7):
                    home_id = df['home_id'].iloc[0]
                    path = f"plots/{community}/{home_id}"
                    plotBasicPeriod(df, path, home_id, "2022-07-04 00:00:00", "2022-07-10 23:59:52")
    # Plot an average for a given date for a community
    if AVERAGE_COMMUNITY:
        plotAverageCommunity("2022-07-04 00:00:00", "2022-07-10 23:59:52")


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

    if REACTION:
        computeAlertReaction()

    # Plot 
    if PLOT:
        allPlots()

    # df = pd.read_csv(RESAMPLED_FOLDER + "/CDB001_2022-05-01--2022-05-31_15min.csv")
    # df = pd.read_csv(DATASET_FOLDER + "/ECHL01.csv")
    # plotBasicPeriod(df, "2022-05-02 00:00:00", "2022-05-08 23:59:52", "15min")

    # Compute the percentage of people that reacted to the message
    # computeStats()


if __name__ == "__main__":
   main()