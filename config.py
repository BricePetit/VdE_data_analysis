import calendar
import copy
import datetime
from dateutil import tz
import matplotlib.dates as dates
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import random

#------------------------------------#
#----------GLOBAL VARIABLES----------#
#------------------------------------#

# Name of the folder where the dataset is located
DATASET_FOLDER = 'dataset'

# Name of the folder where the resampled dataset is located
RESAMPLED_FOLDER = 'resampled_data'

# True if we want to manage the data
MANAGE_DATA = False

# True if we want to verify if there are negative consumptions
VERIFY_CONSUMPTION = False

# True if we want to convert UTC to CET (time)
CONVERT_UTC_CET = False

# True if we want to resample the dataset
RESAMPLE = False

# Set to True if you want to enter in the function to plot
PLOT = False

# True if we want to plot basic information
BASIC_PLOT = False

# True we want to plot the average over the entire community
AVERAGE_COMMUNITY = False

# True if we want to verify reactions
REACTION = True

# True if we want to find the reaction to the sms
GLOBAL_REACTION = True

# Number of minutes that is used during the resampled dataset
# We choose 45 because for each 15 minutes, we have an average.
# (e.g. 00min->14min, 15min->29min, 30min->44min, 45min-> 59min) 
MINUTES = 45

# Name of communities
COMMUNITY_NAME = ["CDB", "ECH"]

# List of period where the consumer need to reduce the consumption for CDB
ALERTS_CDB = [["2022-04-28 19:00:00", "2022-04-28 21:00:00"], 
              ["2022-05-04 14:00:00", "2022-05-04 16:00:00"],
              ["2022-05-15 17:00:00", "2022-05-15 19:00:00"], 
              ["2022-06-03 18:00:00", "2022-06-03 20:00:00"], 
              ["2022-06-11 11:00:00", "2022-06-11 13:00:00"], 
              ["2022-06-23 15:00:00", "2022-06-23 17:00:00"], 
              ["2022-07-12 18:00:00", "2022-07-12 21:00:00"], 
              ["2022-07-20 18:00:00", "2022-07-20 21:00:00"], 
              ["2022-08-02 18:00:00", "2022-08-02 21:00:00"], 
              ["2022-08-19 18:00:00", "2022-08-19 21:00:00"]]

# List of period where the consumer need to reduce the consumption for échappée
ALERTS_ECH = [["2022-05-05 18:00:00", "2022-05-04 21:00:00"],
              ["2022-05-21 11:00:00", "2022-05-21 14:00:00"], 
              ["2022-06-05 10:00:00", "2022-06-05 13:00:00"], 
              ["2022-06-21 17:00:00", "2022-06-21 20:00:00"], 
              ["2022-07-12 16:00:00", "2022-07-12 22:00:00"], 
              ["2022-07-18 14:00:00", "2022-07-18 20:00:00"], 
              ["2022-08-05 15:00:00", "2022-08-05 21:00:00"], 
              ["2022-08-21 15:00:00", "2022-08-21 21:00:00"]]

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
HOURS_MEAN_CDB = {}
HOURS_MEAN_ECHAP = {}

# Dictionary of all houses and the index of the alert
ALERT_REACTION_CDB = {}
ALERT_REACTION_ECH = {}

# Dictionary to rank alerts
RANKING_ALERT_CDB = {}
RANKING_ALERT_ECH = {}
