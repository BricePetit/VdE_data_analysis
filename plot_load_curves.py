from turtle import title
from config import *

#----------------------------------#
#----------PLOT FUNCTIONS----------#
#----------------------------------#

"""
Function to plot the average consumption from the starting date to the ending date for
a given community. The community is ECH or CDB.

:param starting:        The starting date.
:param ending:          The ending date.
:param current_folder:  The folder where we need to consider data.
:param house_nb:        Number of house to apply the average.
:param fmt:             Format used to generate indexes. '15min' or '8S'.
"""
def plotAverageCommunity(starting, ending, current_folder, house_nb, fmt):
    # Create a new dataframe to apply the average
    new_df = pd.DataFrame(columns=['ts','p_cons','p_prod','p_tot'])
    # Count the number of community
    count = 0
    # For file in the folder resampled folder
    for community in ["CDB"]:
        if community == "ECH" and house_nb > 19:
            house_nb = 19
        elif community == "CDB" and house_nb > 28:
            house_nb = 28
        all_files = [f for f in os.listdir(current_folder + '/' + community)]
        chosen_house = random.sample(range(0, len(all_files)), house_nb)
        for house in chosen_house:
            if SEC8:
                # Read the file and create a dataframe
                    df = pd.read_csv(current_folder + '/' + community + '/' + all_files[house])
                    # Query the period on the dataframe.
                    week = df.query("ts >= \"" + starting + "\" and ts <= \"" + ending + "\"")
                    # We check if the new dataframe is not empty
                    if new_df.size != 0:
                        # Add all interesting value to the final dataframe
                        new_df['p_cons'] = new_df['p_cons'] + week['p_cons']
                        new_df['p_prod'] = new_df['p_prod'] + week['p_prod']
                        new_df['p_tot'] = new_df['p_tot'] + week['p_tot']
                    else:
                        # Initialize the dataframe in the case where it is empty
                        new_df = pd.DataFrame({'ts':week['ts'],'p_cons':week['p_cons'],'p_prod':week['p_prod'],
                                                'p_tot':week['p_tot']})
                    count += 1
            else:
                if (all_files[house][:3]  == "CDB" and int(all_files[house][12]) == 5 and int(all_files[house][7:11]) == 2022):
                    # Read the file and create a dataframe
                    df = pd.read_csv(current_folder + '/' + community + '/' + all_files[house])
                    # Query the period on the dataframe.
                    week = df.query("ts >= \"" + starting + "\" and ts <= \"" + ending + "\"")
                    # We check if the new dataframe is not empty
                    if new_df.size != 0:
                        # Add all interesting value to the final dataframe
                        new_df['p_cons'] = new_df['p_cons'] + week['p_cons']
                        new_df['p_prod'] = new_df['p_prod'] + week['p_prod']
                        new_df['p_tot'] = new_df['p_tot'] + week['p_tot']
                    else:
                        # Initialize the dataframe in the case where it is empty
                        new_df = pd.DataFrame({'ts':week['ts'],'p_cons':week['p_cons'],'p_prod':week['p_prod'],
                                                'p_tot':week['p_tot']})
                    count += 1
        # Divide all values by the number of house in the community
        new_df['p_cons'] = new_df['p_cons'] / count
        new_df['p_prod'] = new_df['p_prod'] / count
        new_df['p_tot'] = new_df['p_tot'] / count
        # Plot the results
        plotBasicPeriod(new_df, "plots/" + community + "/average_community", "average_" + community + "_" 
                        + str(house_nb), starting, ending, fmt)

"""
Plot a curve according to the starting and ending date.

:param df:          The dataframe.
:param path:        The pave to save the plot.
:param home_id:     Id of the house in str.
:param starting:    The beginning of the date.
:param ending:      The end of the date.
:param fmt:         Format for indexes. '15min' or '8S'.
"""
def plotBasicPeriod(df, path, home_id, starting, ending, fmt='15min'):
    # Query data for the period.
    week = df.query("ts >= \"" + starting + "\" and ts <= \"" + ending + "\"")
    print(week.head(20))
    # Create time values according to the format for the plot on the x-axis that is 
    # on the bottom
    idx = pd.DatetimeIndex(week['ts'])
    print(idx)
    # Parameter to obtain a large figure
    plt.rcParams["figure.figsize"] = [50, 9]
    # Create the plot
    fig, axs = plt.subplots(nrows=2, ncols=1)

    ax = axs[0]
    # Plot the consumption
    # plot(x='ts', ax=axes[i,j], title=f'Week {week_nb}', ylim=0)
    ax.plot(idx, week['p_cons'])
    # Create a second axis
    ax2 = ax.twiny()
    # Remove useless information on the second axis
    ax2.set(xticklabels=[])
    # Set a limit for the second axis based on the first axis
    ax2.set_xlim(ax.get_xlim())
    # Set the title of this graph
    ax.set_title('p_cons')
    # Set the title of the y-axis
    ax.set_ylabel('Watt')
    # Set the position of the 2e axis
    ax2.xaxis.set_label_position('top')
    # Parameter to plot hours on the 2e axis
    ax2.xaxis.set_minor_locator(dates.HourLocator())
    ax2.xaxis.set_minor_formatter(dates.DateFormatter('%H'))
    ax2.xaxis.grid(True, which="minor")
    # Parameter to plot the date on the first axis
    ax.xaxis.set_major_formatter(dates.DateFormatter(''))

    ax = axs[1]
    # Plot the consumption
    # plot(x='ts', ax=axes[i,j], title=f'Week {week_nb}', ylim=0)
    ax.plot(idx, week['p_tot'])
    # Create a second axis
    ax2 = ax.twiny()
    # Remove useless information on the second axis
    ax2.set(xticklabels=[])
    # Set a limit for the second axis based on the first axis
    ax2.set_xlim(ax.get_xlim())
    # Set the title of this graph
    ax.set_title('p_tot')
    # Set the title of the y-axis
    ax.set_ylabel('Watt')
    # Set the position of the 2e axis
    ax2.xaxis.set_label_position('top')
    # Parameter to plot hours on the 2e axis
    ax2.xaxis.set_minor_locator(dates.HourLocator())
    ax2.xaxis.set_minor_formatter(dates.DateFormatter(''))
    ax2.xaxis.grid(True, which="minor")
    # Parameter to plot the date on the first axis
    ax.xaxis.set_major_formatter(dates.DateFormatter('%d\n%b'))

    # Plot a title
    plt.suptitle(f'Home: {home_id}\nPeriod: {starting} - {ending}\n')
    # Check if the path exists. If it is not the case, we create it
    if not os.path.exists(path):
        os.makedirs(path)
    fig.savefig(f'{path}/{home_id}_{starting[:-9]}_{ending[:-9]}_{fmt}_p_cons-p_tot.png')
    plt.close()
