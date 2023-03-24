__title__ = "config"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


import datetime as dt
import numpy as np
import os
import pandas as pd
import pytz

from typing import List


# ------------------------------------ #
# ----------GLOBAL VARIABLES---------- #
# ------------------------------------ #
# Our timezone
TZ = pytz.timezone('Europe/Brussels')

# Number of workers to parallelize jobs.
NB_SLAVES: int = 8

# True if we want to manage flukso data
FLUKSO: bool = True

# True if we want to manage RTU data
RTU: bool = True

# Set to true if you want to use basic data and False to use resampled data.
BASIC_DATA: bool = True

# Path where we save data
NEXT_CLOUD: str = '/Users/bricepetitulb/Nextcloud/VdE'

# Name of the folder where the dataset is located
DATASET_FOLDER: str = f"{NEXT_CLOUD}/dataset"

# Name of the folder where the resampled dataset is located
RESAMPLED_FOLDER: str = f"{NEXT_CLOUD}/resampled_data"

# Selected folder to work with
CURRENT_FOLDER: str = DATASET_FOLDER if BASIC_DATA else RESAMPLED_FOLDER

# Path to plot data
PLOT_PATH: str = f"{NEXT_CLOUD}/plots"

# True if we want to manage the data
MANAGE_DATA: bool = False

# True if you want to concat data that are recorded in multiple file
CONCAT_DATA: bool = False

# True if we want to verify if there are negative consumptions
VERIFY_CONSUMPTION: bool = True

# True if we want to resample the dataset
RESAMPLE: bool = True

# True if we want to resample the dataset
RESAMPLE_RTU: bool = True

# Set to True if you want to enter in the function to plot
PLOT: bool = True

# Set to True if you want to plot the average over house
PLOT_AVERAGE: bool = True

# Set to True if you want to apply and plot the median, first & third quantile for the RTU
PLOT_MEDIAN_QUANTILE_RTU: bool = True

# Set to True if you want to apply and plot the median, first & third quantile for the flukso
PLOT_MEDIAN_QUANTILE_FLUKSO: bool = True

# Set to True if you want to plot the mean of each wednesday for the flukso
MEAN_WED_FLUKSO: bool = True

# Set to True if you want to plot the mean of each wednesday for the rtu
MEAN_WED_RTU: bool = True

# Set to True if you want to apply a classical plot according to a range
# of dates.
PLOT_RANGE_RTU: bool = True

# True if we want to plot basic information
BASIC_PLOT: bool = True

# True if you want to plot areas
AREA_PLOT: bool = True

# True we want to plot the average over the entire community
AVERAGE_COMMUNITY: bool = True

# True we want to plot the average over all communities
AVERAGE_COMMUNITIES: bool = True

# True we want to plot the aggregation
AGGREGATION: bool = True

# True if we want to verify reactions
REACTION: bool = False

# True if you want to compute the auto consumption
AUTO_CONSUMPTION: bool = True

# True if we want to create a file where we describe if we use data for each ts.
CHECK_DATES: bool = True

# Name of communities
COMMUNITY_NAME: List[str] = ["CDB", "ECH"]

# Temporary dataset containing all information about experiences
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

# List of all data for the CDB
with os.scandir(f"{CURRENT_FOLDER}/CDB") as cdb:
    ALL_CDB: List[str] = sorted([f.name for f in cdb if f.name[:3] == 'CDB'])

# List of commons in ECH
ALL_COMMON: List[str] = ['ECHASC.csv', 'ECHBUA.csv', 'ECHCOM.csv']

# List of all data for the CDB
with os.scandir(f"{CURRENT_FOLDER}/ECH") as ech:
    ALL_ECH: List[str] = sorted(
        [f.name for f in ech if f.name[:3] == 'ECH' and f.name not in ALL_COMMON]
    )

# Matrix containing result of report and reaction
MATRIX_ALERTS_CDB: np.ndarray[np.float64] = np.zeros((
    len(ALL_CDB),
    len(ALERTS_CDB) + (2 * len(REPORTS_HOURS) * len(ALERTS_CDB))
))

MATRIX_ALERTS_ECH: np.ndarray[np.float64] = np.zeros((
    len(ALL_ECH),
    len(ALERTS_ECH) + (2 * len(REPORTS_HOURS) * len(ALERTS_ECH))
))

# List of all consumption during alerts - same period outside alerts
SUM_ALERTS_CDB: np.ndarray[np.float64] = np.zeros(
    len(ALERTS_CDB) + (2 * len(REPORTS_HOURS) * len(ALERTS_CDB))
)
SUM_ALERTS_ECH: np.ndarray[np.float64] = np.zeros(
    len(ALERTS_ECH) + (2 * len(REPORTS_HOURS) * len(ALERTS_ECH))
)
