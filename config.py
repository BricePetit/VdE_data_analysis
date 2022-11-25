__title__ = "config"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


import datetime as dt
import numpy as np
import pandas as pd


# ------------------------------------ #
# ----------GLOBAL VARIABLES---------- #
# ------------------------------------ #

# True if we want to manage flukso data
FLUKSO = False

# True if we want to manage RTU data
RTU = False

# Name of the folder where the dataset is located
DATASET_FOLDER = 'dataset'

# Name of the folder where the resampled dataset is located
RESAMPLED_FOLDER = 'resampled_data'

# True if we want to manage the data
MANAGE_DATA = False

# True if we want to verify if there are negative consumptions
VERIFY_CONSUMPTION = False

# True if we want to resample the dataset
RESAMPLE = False

# True if we want to resample the dataset
RESAMPLE_RTU = False

# Set to True if you want to enter in the function to plot
PLOT = False

# Set to true if you want to use the format 8 sec to plot
SEC8 = False

# True if we want to plot basic information
BASIC_PLOT = False

# True if you want to plot areas
AREA_PLOT = False

# True we want to plot the average over the entire community
AVERAGE_COMMUNITY = False

# True we want to plot the average over all communities
AVERAGE_COMMUNITIES = False

# True we want to plot the aggregation
AGGREGATION = False

# True if we want to verify reactions
REACTION = False

# Name of communities
COMMUNITY_NAME = ["CDB", "ECH"]

tmp_alert_df = pd.read_excel("final_planning.xlsx")

# List of period where the consumer need to reduce the consumption for CDB
ALERTS_CDB = (
    tmp_alert_df[tmp_alert_df["Echappée/Coin du Balai"] == "Coin du Balai"]
    .reset_index(drop=True)
)

# List of period where the consumer need to reduce the consumption for échappée
ALERTS_ECH = (
    tmp_alert_df[tmp_alert_df["Echappée/Coin du Balai"] == "Echappée"]
    .reset_index(drop=True)
)

# Hours to analyze
REPORTS_HOURS = [
    dt.timedelta(hours=3), dt.timedelta(hours=6), dt.timedelta(hours=12)
]

# Matrix containing
# MATRIX_ALERTS_CDB = np.zeros((len(os.listdir(DATASET_FOLDER + '/CDB')),len(ALERTS_CDB)))
# MATRIX_ALERTS_ECH = np.zeros((len(os.listdir(DATASET_FOLDER + '/ECH')),len(ALERTS_ECH)))

MATRIX_ALERTS_CDB = np.zeros((
    len([
        'CDB002', 'CDB006', 'CDB008', 'CDB009', 'CDB011',
        'CDB014', 'CDB030', 'CDB033', 'CDB036', 'CDB042', 'CDB043'
    ]),
    len(ALERTS_CDB) + (2 * len(REPORTS_HOURS) * len(ALERTS_CDB))
))
MATRIX_ALERTS_ECH = np.zeros((
    len(['ECHL01', 'ECHL05', 'ECHL07', 'ECHL08', 'ECHL11', 'ECHL12', 'ECHL13', 'ECHL15', 'ECHL16']),
    len(ALERTS_ECH) + (2 * len(REPORTS_HOURS) * len(ALERTS_ECH))
))

# List of all consumption during alerts - same period outside alerts
SUM_ALERTS_CDB = np.zeros(len(ALERTS_CDB) + (2 * len(REPORTS_HOURS) * len(ALERTS_CDB)))
SUM_ALERTS_ECH = np.zeros(len(ALERTS_ECH) + (2 * len(REPORTS_HOURS) * len(ALERTS_ECH)))
