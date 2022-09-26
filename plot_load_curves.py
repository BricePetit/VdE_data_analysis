__title__ = "plot_load_curves"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


from config import (
    COMMUNITY_NAME,
    SEC8
)


import matplotlib.dates as dates
import matplotlib.pyplot as plt
import os
import pandas as pd
import random


# ---------------------------------- #
# ----------PLOT FUNCTIONS---------- #
# ---------------------------------- #


def create_average_df(starting, ending, current_folder, all_files, house_nb, fmt):
    """
    Function to create an average DataFrame over a period.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param current_folder:  The folder where we need to consider data.
    :param all_files:       List of houses.
    :param house_nb:        Number of house to apply the average.
    :param fmt:             Format used to generate indexes. '15min' or '8S'.

    :return:                Return the averaged dataframe.
    """
    # Set a specific seed to reproduce results
    random.seed(29173946721397129379172391)
    # Create a new dataframe to apply the average
    new_df = pd.DataFrame(columns=['ts', 'p_cons', 'p_prod', 'p_tot'])
    # Select randomly a number of files present in the given list of files
    chosen_house = random.sample(range(0, len(all_files)), house_nb)
    # For each selected house
    for house in chosen_house:
        if SEC8:
            # Read the file and create a dataframe
            df = pd.read_csv(
                current_folder + '/' + all_files[house][:3] + '/' + all_files[house] + '.csv'
            )
        else:
            # Read the file and create a dataframe
            df = pd.read_csv(
                current_folder + '/' + all_files[house][:3] + '/' + all_files[house] + '_'
                + starting[:4] + '_' + starting[6] + '_' + fmt + '.csv'
            )
        # Query the period on the dataframe.
        week = df.query("ts >= \"" + starting + "\" and ts <= \"" + ending + "\"")
        # We check if the new dataframe is not empty
        if new_df.size != 0:
            # Add all interesting value to the final dataframe
            new_df['p_cons'] = new_df['p_cons'] + week['p_cons'].values
            new_df['p_prod'] = new_df['p_prod'] + week['p_prod'].values
            new_df['p_tot'] = new_df['p_tot'] + week['p_tot'].values
        else:
            # Initialize the dataframe in the case where it is empty
            new_df = pd.DataFrame({
                'ts': week['ts'], 'p_cons': week['p_cons'],
                'p_prod': week['p_prod'], 'p_tot': week['p_tot']
            })
    # Divide all values by the number of house in the community
    new_df['p_cons'] = new_df['p_cons'] / house_nb
    new_df['p_prod'] = new_df['p_prod'] / house_nb
    new_df['p_tot'] = new_df['p_tot'] / house_nb

    return new_df


def plot_average_community(starting, ending, current_folder, house_nb, fmt):
    """
    Function to plot the average consumption from the starting date to the ending date for
    a given community. The community is ECH or CDB.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param current_folder:  The folder where we need to consider data.
    :param house_nb:        Number of house to apply the average.
    :param fmt:             Format used to generate indexes. '15min' or '8S'.
    """
    # For file in the folder resampled folder
    for community in COMMUNITY_NAME:
        # Name of all houses that are taken into consideration
        house_name = ""
        if community == 'ECH':
            all_files = [
                'ECHL01', 'ECHL05', 'ECHL07',
                'ECHL08', 'ECHL11', 'ECHL12',
                'ECHL13', 'ECHL15', 'ECHL16'
            ]
        else:
            all_files = [
                'CDB002', 'CDB006', 'CDB008',
                'CDB009', 'CDB011', 'CDB014',
                'CDB030', 'CDB033', 'CDB036',
                'CDB042', 'CDB043'
            ]
        if community == "ECH" and house_nb > 19:
            house_nb = len(all_files)
        elif community == "CDB" and house_nb > 28:
            house_nb = len(all_files)
        # All files in the dataset
        # all_files = [f for f in os.listdir(DATASET_FOLDER + '/' + community)]
        # Temporary files that we want to use
        new_df = create_average_df(starting, ending, current_folder, all_files, house_nb, fmt)
        # Plot the results
        plot_basic_period(
            new_df, "plots/" + community + "/average_community", "average_" + community
            + "_" + str(house_nb) + '_' + house_name, starting, ending, fmt
        )


def average_through_community(starting, ending, current_folder, house_nb, fmt):
    """
    Function to plot the average consumption from the starting date to the ending date.
    We apply the average over all houses in both communities.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param current_folder:  The folder where we need to consider data.
    :param house_nb:        Number of house to apply the average.
    :param fmt:             Format used to generate indexes. '15min' or '8S'.
    """
    # All files in the dataset
    # all_files = [f for f in os.listdir(DATASET_FOLDER + '/ECH')]
    # all_files.append([f for f in os.listdir(DATASET_FOLDER + '/CDB')])
    all_files = [
        'ECHL01', 'ECHL05', 'ECHL07',
        'ECHL08', 'ECHL11', 'ECHL12',
        'ECHL13', 'ECHL15', 'ECHL16',
        'CDB002', 'CDB006', 'CDB008',
        'CDB009', 'CDB011', 'CDB014',
        'CDB030', 'CDB033', 'CDB036',
        'CDB042', 'CDB043'
    ]
    # Check if the size exceed the number of files.
    if house_nb > len(all_files):
        house_nb = len(all_files)
    new_df = create_average_df(starting, ending, current_folder, all_files, house_nb, fmt)
    # Plot the results
    plot_basic_period(
        new_df, "plots/average_community_CDB_ECH", f"{house_nb}_selected_houses", starting,
        ending, fmt
    )


def plot_aggregation(starting, ending, current_folder, fmt):
    """
    Function to plot the aggregation for both communities.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param current_folder:  The folder where we need to consider data.
    :param fmt:             Format used to generate indexes. '15min' or '8S'.
    """
    for community in COMMUNITY_NAME:
        if community == "ECH":
            all_files = ['ECHBUA', 'ECHASC', 'ECHCOM']
        else:
            all_files = ['CDBA01', 'CDBA02']
        new_df = create_average_df(starting, ending, current_folder, all_files, len(all_files), fmt)
        plot_basic_period(
            new_df, "plots/" + community, 'Aggregation', starting, ending, fmt
        )


def plot_basic_period(df, path, home_id, starting, ending, fmt='15min'):
    """
    Plot a curve according to the starting and ending date.

    :param df:          The dataframe.
    :param path:        The pave to save the plot.
    :param home_id:     Id of the house in str.
    :param starting:    The beginning of the date.
    :param ending:      The end of the date.
    :param fmt:         Format for indexes. '15min' or '8S'.
    """
    # Query data for the period.
    week = df.query("ts >= \"" + starting + "\" and ts <= \"" + ending + "\"")
    # Create time values according to the format for the plot on the x-axis that is
    # on the bottom
    idx = pd.DatetimeIndex(week['ts'])
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [9, 9]
    # Create the plot
    fig, axs = plt.subplots(nrows=3, ncols=1)
    for i in range(3):
        # Take figures
        ax = axs[i]
        # Create a second axis
        ax2 = ax.twiny()
        if i == 0:
            # Plot the consumption
            ax.plot(idx, week['p_cons'])
            # Set the title of this graph
            ax.set_title('p_cons')
            # Parameter to plot hours on the 2e axis
            ax2.xaxis.set_minor_locator(dates.HourLocator())
            ax2.xaxis.set_minor_formatter(dates.DateFormatter('%H'))
            ax2.xaxis.grid(True, which="minor")
            # Parameter to plot the date on the first axis
            ax.xaxis.set_major_formatter(dates.DateFormatter(''))

        elif i == 1:
            # Plot the consumption
            ax.plot(idx, week['p_prod'])
            # Set the title of this graph
            ax.set_title('p_prod')
            # Parameter to plot hours on the 2e axis
            ax2.xaxis.set_minor_locator(dates.HourLocator())
            ax2.xaxis.set_minor_formatter(dates.DateFormatter(''))
            ax2.xaxis.grid(True, which="minor")
            # Parameter to plot the date on the first axis
            ax.xaxis.set_major_formatter(dates.DateFormatter(''))

        else:
            # Plot the consumption
            ax.plot(idx, week['p_tot'])
            # Set the title of this graph
            ax.set_title('p_tot')

            # Parameter to plot hours on the 2e axis
            ax2.xaxis.set_minor_locator(dates.HourLocator())
            ax2.xaxis.set_minor_formatter(dates.DateFormatter(''))
            ax2.xaxis.grid(True, which="minor")
            # Parameter to plot the date on the first axis
            ax.xaxis.set_major_formatter(dates.DateFormatter('%d\n%b'))
        # Plot a line to distinguish the 0
        ax.axhline(y=0, color="black", linestyle="--")
        # Remove useless information on the second axis
        ax2.set(xticklabels=[])
        # Set a limit for the second axis based on the first axis
        ax2.set_xlim(ax.get_xlim())
        # Set the title of the y-axis
        ax.set_ylabel('Watt')
        # Set the position of the 2e axis
        ax2.xaxis.set_label_position('top')
    # Plot a title
    plt.suptitle(f'Home: {home_id}\nPeriod: {starting} - {ending}\n')
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path):
        os.makedirs(path)
    fig.savefig(f'{path}/{home_id}_{starting[:-9]}_{ending[:-9]}_{fmt}.png')
    plt.close()
