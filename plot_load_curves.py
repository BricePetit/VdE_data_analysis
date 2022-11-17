__title__ = "plot_load_curves"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"

from config import (
    LOCAL_TZ,
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
        plot_data(
            new_df, f"plots/{community}/average_community", starting, ending,
            f"average_{community}_{house_nb}_{house_name}", 'multiple_flukso', fmt
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
    plot_data(
        new_df, "plots/average_community_CDB_ECH", starting, ending,
        f"{house_nb}_selected_houses", 'multiple_flukso', fmt
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
        plot_data(
            new_df, f"plots/{community}", starting, ending,
            'Aggregation', 'multiple_flukso', fmt
        )


def flukso_basic_plot(idx, week, ax, i=None):
    """
    Function to plot flukso data.

    :param idx:     DataIndex.
    :param week:    DataFrame containing data of a week.
    :param ax:      1er axis.
    :param i=None:  Optional parameter used in the case where we want to plot 3 subplots.
    """
    # Plot a line to distinguish the 0
    ax.axhline(y=0, color="black", linestyle="--")
    # Plot the classical way
    if i is None:
        ax.plot(idx, week['p_tot'], color="gray")
        ax.plot(idx, week['p_cons'], color="red")
        ax.plot(idx, week['p_prod'], color="blue")
        # Fill areas
        ax.fill_between(idx, week['p_tot'], where=week['p_tot'] > 0, color="lightyellow")
        ax.fill_between(idx, week['p_tot'], where=week['p_tot'] < 0, color="palegreen")
        # Create the legend
        ax.legend(
            ['Consumption', 'Production', 'Total power', 'Withdrawal', 'Feeding-in'],
            bbox_to_anchor=[0.5, 1]
        )
    # Plot consumption, production & total in subplot
    else:
        # Plot the consumption in one subgraph
        if i == 0:
            # Plot the consumption
            ax.plot(idx, week['p_cons'])
            # Set the title of this graph
            ax.set_title('Consumption power')
        # Plot the production in another subgraph
        elif i == 1:
            # Plot the production
            ax.plot(idx, week['p_prod'])
            # Set the title of this graph
            ax.set_title('Production power')
        # Plot the total consumption in the last subgraph
        else:
            # Plot the total power
            ax.plot(idx, week['p_tot'])
            # Set the title of this graph
            ax.set_title('Total power')


def rtu_basic_plot(idx, week, ax):
    """
    Function to plot rtu data.

    :param idx:     DataIndex.
    :param week:    DataFrame containing data of a week.
    :param ax:      1er axis.
    """
    # Plot values
    ax.plot(idx, week['active'], color="red")
    # Fill areas
    ax.fill_between(idx, week['active'], where=week['active'] > 0, color="lightyellow")
    # Create the legend
    ax.legend(['Active power', 'Withdrawal'], bbox_to_anchor=[0.5, 1])


def plot_formatter(ax, ax2, i=None):
    """
    Function to format the two axis on the graph.

    :param ax:      1er axis.
    :param ax2:     2e axis.
    :param i=None:  Optional parameter used in the case where we want to plot 3 subplots.
    """
    # Create line separation on the plot
    ax.xaxis.grid(True, which="both")
    # Parameter to plot hours on the first axis
    ax.xaxis.set_minor_locator(dates.HourLocator(tz=LOCAL_TZ))
    # Parameter to plot days on the 2e axis
    ax2.xaxis.set_major_locator(dates.DayLocator(tz=LOCAL_TZ))
    if i is None:
        # Parameter to plot hours on the first axis
        ax.xaxis.set_minor_formatter(dates.DateFormatter('%H', tz=LOCAL_TZ))
        # Parameter to plot days on the 2e axis
        ax2.xaxis.set_major_formatter(dates.DateFormatter('%d\n%b', tz=LOCAL_TZ))
        # Set the position of the 2e axis
        ax2.xaxis.set_label_position('top')
    else:
        ax.xaxis.set_minor_formatter(dates.DateFormatter('', tz=LOCAL_TZ))
        # Parameter to plot the date on the first axis
        ax2.xaxis.set_major_formatter(dates.DateFormatter('', tz=LOCAL_TZ))
        # Formatter for the plot the consumption, we put time
        if i == 0:
            # Parameter to plot the date on the first axis
            ax2.xaxis.set_major_formatter(dates.DateFormatter('%d\n%b', tz=LOCAL_TZ))
            # Set the position of the 2e axis
            ax2.xaxis.set_label_position('top')
        # Formatter for the plot the consumption, we put days
        elif i == 2:
            # Parameter to plot hours on the 1er axis
            ax.xaxis.set_minor_formatter(dates.DateFormatter('%H', tz=LOCAL_TZ))


def plot_data(df, path, starting, ending, title, plot_type, fmt='15min'):
    """
    Plot all curves according to the starting and ending date. Plot also areas
    between total power curve.

    :param df:          The dataframe.
    :param path:        The pave to save the plot.
    :param home_id:     Id of the house in str.
    :param starting:    The beginning of the date.
    :param ending:      The end of the date.
    :param fmt=15min:   Format for indexes. '15min' or '8S'.
    """
    str_starting = starting.isoformat(sep=" ")
    str_ending = ending.isoformat(sep=" ")
    # Query data for the period.
    week = df.query(
        "ts >= \"" + str_starting + "\" and ts <= \"" + str_ending + "\""
    )
    # Create time values according to the format for the plot on the x-axis that is
    # on the bottom
    idx = pd.DatetimeIndex(week['ts'])
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [9 * ((ending - starting).days + 1), 9]
    # Plot data
    if plot_type == "multiple_flukso":
        # Create the plot
        fig, axs = plt.subplots(nrows=3, ncols=1)
        for i in range(3):
            # Take figures
            ax = axs[i]
            # Create a second axis
            ax2 = ax.twiny()
            # Plot data
            flukso_basic_plot(idx, week, ax, i)
            # Format axis
            plot_formatter(ax, ax2, i)
            # Remove useless information on the second axis
            ax.set(xticklabels=[])
            # Set a limit for the second axis based on the first axis
            ax2.set_xlim(ax.get_xlim())
    else:
        # Create the plot
        fig, ax = plt.subplots()
        # Create a second axis
        ax2 = ax.twiny()
        if plot_type == "rtu":
            # Plot rtu data
            rtu_basic_plot(idx, week, ax)
        elif plot_type == "flukso":
            # Plot flukso data
            flukso_basic_plot(idx, week, ax)
        # Format axis
        plot_formatter(ax, ax2)
        # Remove useless information on the second axis
        ax.set(xticklabels=[])
        # Set a limit for the second axis based on the first axis
        ax2.set_xlim(ax.get_xlim())
    # Set the title of the y-axis
    ax.set_ylabel('Watt')
    # Plot a title
    plt.suptitle(title + f"\nPeriod: {starting} - {ending}\n")
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path):
        os.makedirs(path)
    fig.savefig(
        f"{path}/{plot_type}_{str_starting[:-15]}_{str_ending[:-15]}_{fmt}.png"
    )
    plt.close()


def main():
    house = 'CDB005'
    start_date = '2022-11-03 '
    end_date = '2022-11-03 '
    file = f'{house}_{start_date[:-1]}.csv'
    df = pd.read_csv('/Users/bricepetitulb/Library/CloudStorage/OneDrive-UniversitéLibredeBruxelles/Ulb/PhD/VdE/Voisin-d-energie-ULB/test_csv/' + file)
    plot_data(df, 'test2', house, start_date + '00:00:00', end_date + '23:59:52', '8S')


if __name__ == "__main__":
    main()
