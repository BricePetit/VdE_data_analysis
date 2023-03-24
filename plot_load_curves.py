__title__ = "plot_load_curves"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"

from config import (
    COMMUNITY_NAME,
    BASIC_DATA,
    PLOT_PATH,
    CURRENT_FOLDER,
    ALL_ECH,
    ALL_CDB
)


import datetime as dt
import matplotlib.dates as dates
import matplotlib.pyplot as plt
import os
import pandas as pd
import random
from typing import NoReturn, Optional, List, Tuple


# ---------------------------------- #
# ----------PLOT FUNCTIONS---------- #
# ---------------------------------- #


def create_average_df(
    starting: dt.datetime,
    ending: dt.datetime,
    all_files: List[str],
    house_nb: int
) -> Tuple[pd.DataFrame, List[int]]:
    """
    Function to create an average DataFrame over a period.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param all_files:       List of houses.
    :param house_nb:        Number of house to apply the average.

    :return:                Return the averaged dataframe and a list of selected houses.
    """
    # Set a specific seed to reproduce results
    random.seed(29173946721397129379172391)
    # Create a new dataframe to apply the average
    new_df: pd.DataFrame = pd.DataFrame(columns=['ts', 'p_cons', 'p_prod', 'p_tot'])
    # Select randomly a number of files present in the given list of files
    chosen_house = random.sample(range(0, len(all_files)), house_nb)
    # For each selected house
    for house in chosen_house:
        print(f'--------------{all_files[house]}--------------')
        # Read the file and create a dataframe
        if BASIC_DATA:
            df: pd.DataFrame = pd.read_csv(
                f"{CURRENT_FOLDER}/{all_files[house][:3]}/{all_files[house]}.csv"
            )
        else:
            df: pd.DataFrame = pd.read_csv(
                f"{CURRENT_FOLDER}/{all_files[house][:3]}/{all_files[house]}_15min.csv"
            )
        # Query the period on the dataframe.
        week: pd.DataFrame = df.query(f"ts >= '{starting}' and ts <= '{ending}'")
        # Concat all houses
        new_df: pd.DataFrame = pd.concat([new_df, week])
    # Apply the mean
    new_df: pd.DataFrame = new_df.groupby('ts').mean(numeric_only=True).reset_index()
    return new_df, chosen_house


def plot_average_community(
    starting: dt.datetime,
    ending: dt.datetime,
    house_nb: int,
    fmt: str
) -> NoReturn:
    """
    Function to plot the average consumption from the starting date to the ending date for
    a given community. The community is ECH or CDB.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param current_folder:  The folder where we need to consider data.
    :param house_nb:        Number of house to apply the average.
    :param fmt:             The data format.
    """
    # For file in the folder resampled folder
    for community in COMMUNITY_NAME:
        # Name of all houses that are taken into consideration
        if community == 'ECH':
            house_name: str = "ECH_"
            all_files: List[str] = ALL_ECH
        else:
            house_name: str = "CDB_"
            all_files: List[str] = ALL_CDB
        if house_nb > len(all_files):
            house_nb: int = len(all_files)
        # Temporary files that we want to use
        new_df, selected_houses = create_average_df(starting, ending, all_files, house_nb)
        for i in selected_houses:
            house_name += all_files[i][-3:] + '_'
        # Plot the results
        plot_data(
            new_df, f"{PLOT_PATH}/{community}/average_community", starting, ending,
            f"average in {community} over {house_nb} houses", 'flukso',
            f"average_{starting.date()}_{community}_{house_nb}_{house_name}_{fmt}"
        )


def average_through_community(
    starting: dt.datetime,
    ending: dt.datetime,
    house_nb: int,
    fmt: str
) -> NoReturn:
    """
    Function to plot the average consumption from the starting date to the ending date.
    We apply the average over all houses in both communities.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param house_nb:        Number of house to apply the average.
    :param fmt:             The data format.
    """
    # All files in the dataset
    # all_files = [f for f in os.listdir(DATASET_FOLDER + '/ECH')]
    # all_files.append([f for f in os.listdir(DATASET_FOLDER + '/CDB')])
    all_files: List[str] = ALL_CDB + ALL_ECH
    # Check if the size exceed the number of files.
    if house_nb > len(all_files):
        house_nb: int = len(all_files)
    new_df, _ = create_average_df(starting, ending, all_files, house_nb)
    # Plot the results
    plot_data(
        new_df, f"{PLOT_PATH}/average_community_CDB_ECH", starting, ending,
        f"{house_nb} selected houses", 'flukso', f"{house_nb}_selected_houses_{starting.date()}_{fmt}"
    )


def plot_aggregation(
    starting: dt.datetime,
    ending: dt.datetime,
) -> NoReturn:
    """
    Function to plot the aggregation for both communities.

    :param starting:        The starting date.
    :param ending:          The ending date.
    :param current_folder:  The folder where we need to consider data.
    """
    for community in COMMUNITY_NAME:
        if community == "ECH":
            all_files: List[str] = ['ECHBUA', 'ECHASC', 'ECHCOM']
        else:
            all_files: List[str] = ['CDBA01', 'CDBA02']
        new_df, _ = create_average_df(starting, ending, all_files, len(all_files))
        plot_data(
            new_df, f"{PLOT_PATH}/{community}", starting, ending,
            'Aggregation', 'multiple_flukso'
        )


def flukso_basic_plot(
    idx: pd.DatetimeIndex,
    week: pd.DataFrame,
    ax: plt.Axes,
    i: Optional[int] = None
) -> NoReturn:
    """
    Function to plot flukso data.

    :param idx:     DataIndex.
    :param week:    DataFrame containing data of a week.
    :param ax:      1er axis.
    :param i=None:  Optional parameter used in the case where we want to plot 3 subplots.
    """
    # Plot the classical way
    if i is None:
        ax.plot(idx, week['p_tot'], color="gray", label='Total power')
        ax.plot(idx, week['p_cons'], color="red", label='Consumption')
        ax.plot(idx, week['p_prod'], color="blue", label='Production')
        # Fill areas
        ax.fill_between(
            idx, week['p_tot'], where=week['p_tot'] > 0,
            color="lightyellow", label='Withdrawal'
        )
        ax.fill_between(
            idx, week['p_tot'], where=week['p_tot'] < 0,
            color="palegreen", label='Feeding-in'
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
    # Plot a line to distinguish the 0
    ax.axhline(y=0, color="black", linestyle="--")
    # Show the legend
    ax.legend()


def rtu_basic_plot(idx: pd.DatetimeIndex, df: pd.DataFrame, ax: plt.Axes) -> NoReturn:
    """
    Function to plot rtu data.

    :param idx: DataIndex.
    :param df:  DataFrame containing data of a week.
    :param ax:  1er axis.
    """
    # Plot values
    ax.plot(idx, df['active'], color="red", label='Active power')
    # Fill areas
    ax.fill_between(
        idx, df['active'], where=df['active'] > 0,
        color="lightyellow", label='Withdrawal'
    )
    # Show the legend
    ax.legend()


def rtu_plot(df: pd.DataFrame, ts_series: pd.Series, path_to_save: str, title: str) -> NoReturn:
    """
    Function to do a plot of RTU.

    :param df:              Dataframe to plot.
    :param ts_series:       Series for the index.
    :param path_to_save:    Path to save the plot.
    :param title:           Title of the plot.
    """
    idx: pd.DatetimeIndex = pd.DatetimeIndex(ts_series)
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [10, 8]
    # Plot data
    # Create the plot
    fig, ax = plt.subplots()
    # Plot line to recognize the 0
    ax.axhline(y=0, color="black", linestyle="--")
    rtu_basic_plot(idx, df, ax)
    # Create line separation on the plot
    ax.xaxis.grid(True, which="major")
    # Parameter to plot hours on the first axis
    ax.xaxis.set_major_locator(dates.HourLocator())
    ax.xaxis.set_major_formatter(dates.DateFormatter('%H'))
    # Set a limit for the second axis based on the first axis
    # Set the title of the x/y-axis
    ax.set_xlabel('Time (Hour)')
    ax.set_ylabel('Power (Watt)')
    # Plot a title
    plt.suptitle(title)
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
    # Save the fig
    fig.savefig(
        f"{path_to_save}/rtu_mean_wednesday.png"
    )
    plt.close()


def flukso_plot(df: pd.DataFrame, ts_series: pd.Series, path_to_save: str, title: str) -> NoReturn:
    """
    Function to do a plot of flukso.

    :param df:              Dataframe to plot.
    :param ts_series:       Series for the index.
    :param path_to_save:    Path to save the plot.
    :param title:           Title of the plot.
    """
    idx: pd.DatetimeIndex = pd.DatetimeIndex(ts_series)
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [10, 8]
    # Plot data
    # Create the plot
    fig, ax = plt.subplots()
    # Plot line to recognize the 0
    ax.axhline(y=0, color="black", linestyle="--")
    flukso_basic_plot(idx, df, ax)
    # Create line separation on the plot
    ax.xaxis.grid(True, which="major")
    # Parameter to plot hours on the first axis
    ax.xaxis.set_major_locator(dates.HourLocator())
    ax.xaxis.set_major_formatter(dates.DateFormatter('%H'))
    # Set a limit for the second axis based on the first axis
    # Set the title of the x/y-axis
    ax.set_xlabel('Time (Hour)')
    ax.set_ylabel('Power (Watt)')
    # Plot a title
    plt.suptitle(title)
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
    # Save the fig
    fig.savefig(
        f"{path_to_save}/{path_to_save[-6:]}_mean_wednesday.png"
    )
    plt.close()


def plot_formatter(
    ax: plt.Axes,
    ax2: plt.Axes,
    tz: dt.timezone,
    i: Optional[int] = None
) -> NoReturn:
    """
    Function to format the two axis on the graph.

    :param ax:      1er axis.
    :param ax2:     2e axis.
    :param tz:      Timezone.
    :param i=None:  Optional parameter used in the case where we want to plot 3 subplots.
    """
    # Create line separation on the plot
    ax.xaxis.grid(True, which="major")
    # Parameter to plot hours on the first axis
    ax.xaxis.set_major_locator(dates.HourLocator(tz=tz))
    # Parameter to plot days on the 2e axis
    ax2.xaxis.set_major_locator(dates.DayLocator(tz=tz))
    if i is None:
        # Parameter to plot hours on the first axis
        ax.xaxis.set_major_formatter(dates.DateFormatter('%H', tz=tz))
        # Parameter to plot days on the 2e axis
        ax2.xaxis.set_major_formatter(dates.DateFormatter('%d\n%b', tz=tz))
        # Set the position of the 2e axis
        ax2.xaxis.set_label_position('top')
    else:
        ax.xaxis.set_major_formatter(dates.DateFormatter('', tz=tz))
        # Parameter to plot the date on the first axis
        ax2.xaxis.set_major_formatter(dates.DateFormatter('', tz=tz))
        # Formatter for the plot the consumption, we put time
        if i == 0:
            # Parameter to plot the date on the first axis
            ax2.xaxis.set_major_formatter(dates.DateFormatter('%d\n%b', tz=tz))
            # Set the position of the 2e axis
            ax2.xaxis.set_label_position('top')
        # Formatter for the plot the consumption, we put days
        elif i == 2:
            # Parameter to plot hours on the 1er axis
            ax.xaxis.set_major_formatter(dates.DateFormatter('%H', tz=tz))


def plot_data(
    df: pd.DataFrame,
    path: str,
    starting: dt.datetime,
    ending: dt.datetime,
    title: str,
    plot_type: str,
    file_name: str,
) -> NoReturn:
    """
    Plot all curves according to the starting and ending date. Plot also areas
    between total power curve.

    :param df:          The dataframe.
    :param path:        The pave to save the plot.
    :param starting:    The beginning of the date.
    :param ending:      The end of the date.
    :param title:       Title of the fig.
    :param plot_type:   Type of the plot.
    :param file_name:   The name of the file.
    """
    tz = starting.tzinfo
    str_starting: str = starting.isoformat(sep=" ")
    str_ending: str = ending.isoformat(sep=" ")
    # Query data for the period.
    week: pd.DataFrame = df.query(
        "ts >= \"" + str_starting + "\" and ts <= \"" + str_ending + "\""
    )
    # Create time values according to the format for the plot on the x-axis that is
    # on the bottom
    idx: pd.DatetimeIndex = pd.DatetimeIndex(week['ts'])
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [10 * ((ending - starting).days + 1), 8]
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
            plot_formatter(ax, ax2, tz, i)
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
        plot_formatter(ax, ax2, tz)
        # Set a limit for the second axis based on the first axis
        ax2.set_xlim(ax.get_xlim())
    # Set the title of the x/y-axis
    ax.set_xlabel('Time (Hour)')
    ax.set_ylabel('Power (Watt)')
    # Plot a title
    plt.suptitle(title + f"\nPeriod: {starting} - {ending}\n")
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path):
        os.makedirs(path)
    # Save the fig
    fig.savefig(f"{path}/{file_name}.png")
    plt.close()


def plot_median_quantile(
    mean: pd.DataFrame,
    first_q: pd.DataFrame,
    third_q: pd.DataFrame,
    col_name: str,
    ts_series: pd.Series,
    title: str,
    path: str
) -> NoReturn:
    """
    Function to plot the median, the first quantile and the third quantile.

    :param mean:        Series with the mean.
    :param first_q:     Series with the first quantile.
    :param third_q:     Series with the third quantile.
    :param col_name:    The name of the column.
    :param ts_series:   Series containing all hours to plot.
    :param title:       Title of the plot.
    :param path:        Path to save the file.
    """
    # Create time values according to the format for the plot on the x-axis that is
    # on the bottom
    idx: pd.DatetimeIndex = pd.DatetimeIndex(ts_series)
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [10, 8]
    # Plot data
    # Create the plot
    fig, ax = plt.subplots()
    # Plot line to recognize the 0
    ax.axhline(y=0, color="black", linestyle="--")
    # ax.plot(idx, std, color="gray")
    ax.errorbar(idx, mean, yerr=[first_q, third_q], capsize=2)
    # Create line separation on the plot
    ax.xaxis.grid(True, which="major")
    # Parameter to plot hours on the first axis
    ax.xaxis.set_major_locator(dates.HourLocator())
    ax.xaxis.set_major_formatter(dates.DateFormatter('%H'))
    # Set a limit for the second axis based on the first axis
    # Set the title of the x/y-axis
    ax.set_xlabel('Time (Hour)')
    ax.set_ylabel('Power (Watt)')
    # if col_name == "p_cons":
    #     plt.ylim(bottom=-0.1)
    # elif col_name == "p_prod":
    #     plt.ylim(top=0.1)
    # else:
    #     if all(value > 0 for value in mean):
    #         plt.ylim(bottom=-0.1)
    # Plot a title
    plt.suptitle(title)
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path):
        os.makedirs(path)
    # Save the fig
    if col_name == 'rtu':
        fig.savefig(
            f"{path}/{col_name}_median_quantile_.png"
        )
    else:
        fig.savefig(
            f"{path}/{title[-6:]}_median_quantile_{col_name}.png"
        )
    # plt.show()
    plt.close()


def plot_median_quantile_rtu(df: pd.DataFrame, plot_path: str) -> NoReturn:
    """
    Function to plot the median, first and third quantile for the rtu data.

    :param df:          DataFrame.
    :param plot_path:   Path to save the plot.
    """
    time_series: pd.Series = (
        pd.date_range("00:00:00", freq='15min', periods=96)
        .to_series()
        .apply(lambda x: x.strftime('%H:%M:%S'))
        .reset_index(drop=True)
    )
    median: pd.DataFrame = df.groupby(df['ts'].dt.time).median(numeric_only=True)
    first_q: pd.DataFrame = df.groupby(df['ts'].dt.time).quantile(q=0.25, numeric_only=True)
    third_q: pd.DataFrame = df.groupby(df['ts'].dt.time).quantile(q=0.75, numeric_only=True)
    title: str = "Low voltage cabin (RTU) - active power"
    plot_median_quantile(
        median['active'], first_q['active'], third_q['active'], 'rtu', time_series, title, plot_path
    )


def plot_median_quantile_flukso(df: pd.DataFrame, plot_path: str) -> NoReturn:
    """
    Function to plot the median, first and third quantile flukso data.

    :param df:          DataFrame.
    :param plot_path:   Path.
    """
    # Create a series
    time_series: pd.Series = (
        pd.date_range("00:00:00", freq='15min', periods=96)
        .to_series()
        .apply(lambda x: x.strftime('%H:%M:%S'))
        .reset_index(drop=True)
    )
    median: pd.DataFrame = df.groupby(df['ts'].dt.time).median(numeric_only=True)
    first_q: pd.DataFrame = df.groupby(df['ts'].dt.time).quantile(q=0.25, numeric_only=True).abs()
    third_q: pd.DataFrame = df.groupby(df['ts'].dt.time).quantile(q=0.75, numeric_only=True).abs()
    for col in ['p_cons', 'p_prod', 'p_tot']:
        if col == 'p_cons':
            title: str = f"House's consumption: {df['home_id'].iloc[0]}"
        elif col == 'p_prod':
            title: str = f"House's production: {df['home_id'].iloc[0]}"
        else:
            title: str = f"House's total power: {df['home_id'].iloc[0]}"
        plot_median_quantile(
            median[col], first_q[col], third_q[col], col, time_series, title, plot_path
        )
