__title__ = "config"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


import datetime as dt
import numpy as np
import pandas as pd
import pytz

from typing import List


# ------------------------------------ #
# ----------GLOBAL VARIABLES---------- #
# ------------------------------------ #
# Our timezone
TZ = pytz.timezone('Europe/Brussels')

# Number of workers to parallelize jobs.
NB_SLAVES = 8

# True if we want to manage flukso data
FLUKSO: bool = False

# True if we want to manage RTU data
RTU: bool = False

# Name of the folder where the dataset is located
DATASET_FOLDER: str = 'dataset'

# Name of the folder where the resampled dataset is located
RESAMPLED_FOLDER: str = 'resampled_data'

# True if we want to manage the data
MANAGE_DATA: bool = False

# True if you want to concat data that are recorded in multiple file
CONCAT_DATA: bool = False

# True if we want to verify if there are negative consumptions
VERIFY_CONSUMPTION: bool = False

# True if we want to resample the dataset
RESAMPLE: bool = False

# True if we want to resample the dataset
RESAMPLE_RTU: bool = False

# Set to True if you want to enter in the function to plot
PLOT: bool = False

# Set to True if you want to apply and plot the median, first & third quantile for the RTU
PLOT_MEDIAN_QUANTILE_RTU: bool = False

# Set to True if you want to apply and plot the median, first & third quantile for the flukso
PLOT_MEDIAN_QUANTILE_FLUKSO: bool = False

# Set to True if you want to plot the mean of each wednesday for the flukso
MEAN_WED_FLUKSO: bool = False

# Set to True if you want to plot the mean of each wednesday for the rtu
MEAN_WED_RTU: bool = False

# Set to True if you want to apply a classical plot according to a range
# of dates.
PLOT_RANGE_RTU: bool = False

# Set to true if you want to use the format 8 sec to plot
SEC8: bool = False

# True if we want to plot basic information
BASIC_PLOT: bool = False

# True if you want to plot areas
AREA_PLOT: bool = False

# True we want to plot the average over the entire community
AVERAGE_COMMUNITY: bool = False

# True we want to plot the average over all communities
AVERAGE_COMMUNITIES: bool = False

# True we want to plot the aggregation
AGGREGATION: bool = False

# True if we want to verify reactions
REACTION: bool = False

# Name of communities
COMMUNITY_NAME: List[str] = ["CDB", "ECH"]

tmp_alert_df: pd.DataFrame = pd.read_excel("final_planning.xlsx")

# List of period where the consumer need to reduce the consumption for CDB
ALERTS_CDB: pd.DataFrame = (
    tmp_alert_df[tmp_alert_df["Echappée/Coin du Balai"] == "Coin du Balai"]
    .reset_index(drop=True)
)

# List of period where the consumer need to reduce the consumption for échappée
ALERTS_ECH: pd.DataFrame = (
    tmp_alert_df[tmp_alert_df["Echappée/Coin du Balai"] == "Echappée"]
    .reset_index(drop=True)
)

# Hours to analyze
REPORTS_HOURS: List[dt.timedelta] = [
    dt.timedelta(hours=3), dt.timedelta(hours=6), dt.timedelta(hours=12)
]

# MATRIX_ALERTS_CDB = np.zeros((len(os.listdir(DATASET_FOLDER + '/CDB')),len(ALERTS_CDB)))
# MATRIX_ALERTS_ECH = np.zeros((len(os.listdir(DATASET_FOLDER + '/ECH')),len(ALERTS_ECH)))

# List of all considered data for the CDB in the report
REPORT_CDB: List[str] = [
    'CDB002', 'CDB003', 'CDB004', 'CDB009',
    'CDB011', 'CDB016', 'CDB019', 'CDB021',
    'CDB024', 'CDB025', 'CDB027', 'CDB028',
    'CDB035', 'CDB036', 'CDB040', 'CDB042',
    'CDB049', 'CDB054', 'CDB059'
]

# List of all considered data for the CDB
ALL_CDB: List[str] = [
    'CDB002', 'CDB003', 'CDB004', 'CDB005',
    'CDB009', 'CDB011', 'CDB012', 'CDB014',
    'CDB016', 'CDB019', 'CDB020', 'CDB021',
    'CDB022', 'CDB024', 'CDB025', 'CDB027',
    'CDB028', 'CDB030', 'CDB035', 'CDB036',
    'CDB037', 'CDB040', 'CDB042', 'CDB045',
    'CDB046', 'CDB047', 'CDB048', 'CDB049',
    'CDB051', 'CDB054', 'CDB059'
]

# List of all considered data for the ECH in the report
REPORT_ECH: List[str] = [
    'ECHL01', 'ECHL04', 'ECHL05',
    'ECHL07', 'ECHL08', 'ECHL11', 'ECHL12',
    'ECHL15', 'ECHL17', 'ECHL2A'
]

# List of all considered data for the ECH
ALL_ECH: List[str] = [
    'ECHL01', 'ECHL04', 'ECHL05', 'ECHL06',
    'ECHL07', 'ECHL08', 'ECHL11', 'ECHL12',
    'ECHL13', 'ECHL15', 'ECHL16', 'ECHL17', 'ECHL2A'
]

# Matrix containing result of report and reaction
MATRIX_ALERTS_CDB = np.zeros((
    len(ALL_CDB),
    len(ALERTS_CDB) + (2 * len(REPORTS_HOURS) * len(ALERTS_CDB))
))

MATRIX_ALERTS_ECH = np.zeros((
    len(ALL_ECH),
    len(ALERTS_ECH) + (2 * len(REPORTS_HOURS) * len(ALERTS_ECH))
))

# List of all consumption during alerts - same period outside alerts
SUM_ALERTS_CDB = np.zeros(
    len(ALERTS_CDB) + (2 * len(REPORTS_HOURS) * len(ALERTS_CDB))
)
SUM_ALERTS_ECH = np.zeros(
    len(ALERTS_ECH) + (2 * len(REPORTS_HOURS) * len(ALERTS_ECH))
)
