__title__ = "main"
__version__ = "1.0.0"
__author__ = "Brice Petit"
__license__ = "MIT"


from plotLoadCurves import *
from smsReaction import *
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
    for community in COMMUNITY_NAME:
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
        i = -1
        previous_file = []
        for file in os.listdir(path):
            print("---------------"+ file[:6] +"---------------")
            df = pd.read_csv(path + '/' + file)
            # Find if a house reacted to the message
            if GLOBAL_REACTION:
                if community == "CDB" and file[:6] in ['CDB002', 'CDB006', 'CDB008', 'CDB009', 'CDB011', 'CDB014', 'CDB030', 'CDB033', 'CDB036', 'CDB042', 'CDB043']:
                    if file[:6] not in previous_file:
                        previous_file.append(file[:6])
                        i += 1
                    findGlobalReactionAndReport(df, file, path, ALERTS_CDB, ALERT_REACTION_CDB, RANKING_ALERT_CDB, MATRIX_ALERTS_CDB, SUM_ALERTS_CDB, i)
                elif community == "ECH" and file[:6] in ['ECHL01', 'ECHL05', 'ECHL07', 'ECHL08', 'ECHL11', 'ECHL12', 'ECHL13', 'ECHL15', 'ECHL16']:
                    if file[:6] not in previous_file:
                        previous_file.append(file[:6])
                        i += 1
                    findGlobalReactionAndReport(df, file, path, ALERTS_ECH, ALERT_REACTION_ECH, RANKING_ALERT_ECH, MATRIX_ALERTS_ECH, SUM_ALERTS_ECH, i)
                
    
    print()
    print("----------CDB ALERTS RANKING----------")
    for key in RANKING_ALERT_CDB:
        print("The alert", key, f"{ALERTS_CDB[key]}", "obtained", RANKING_ALERT_CDB[key], "reactions.")
    print()
    print("----------CDB ALERTS RANKING----------")
    for key in ALERT_REACTION_CDB:
        print(f"The home id {key} reacted to the following alerts {ALERT_REACTION_CDB[key]}.")
        print(f"The home id {key} reacted to {len(ALERT_REACTION_CDB[key])/len(ALERTS_CDB)*100}% of the alerts.")
    
    # Take all home ids
    cdb_home_id = ['CDB002', 'CDB006', 'CDB008', 'CDB009', 'CDB011', 'CDB014', 'CDB030', 'CDB033', 'CDB036', 'CDB042', 'CDB043']
    ech_home_id = ['ECHL01', 'ECHL05', 'ECHL07', 'ECHL08', 'ECHL11', 'ECHL12', 'ECHL13', 'ECHL15', 'ECHL16']
    # cdb_home_id = [f[:6] for f in os.listdir(DATASET_FOLDER + '/CDB')]
    # ech_home_id = [f[:6] for f in os.listdir(DATASET_FOLDER + '/ECH')]

    # Export MATRIX_ALERTS_CDB or MATRIX_ALERTS_ECH in excel files
    exportToXLSX(MATRIX_ALERTS_CDB, cdb_home_id, ALERTS_CDB, SUM_ALERTS_CDB, "plots/alerts_cdb.xlsx")
    exportToXLSX(MATRIX_ALERTS_ECH, ech_home_id, ALERTS_ECH, SUM_ALERTS_ECH, "plots/alerts_ech.xlsx")

    # Plot the matrix
    # plot_confusion_matrix(MATRIX_ALERTS_CDB, ALERTS_CDB, cdb_home_id)
    # plot_confusion_matrix(MATRIX_ALERTS_ECH, ALERTS_ECH, ech_home_id)
    # plt.show()


def plot_confusion_matrix(matrix, alerts, home_ids, title='Confusion matrix', cmap=plt.cm.Oranges):
    alerts = ['A'+ str(i+1) for i in range(len(alerts))]
    # create a new figure
    fig, ax = plt.subplots()


    # 111: 1x1 grid, first subplot
    # ax = fig.add_subplot()
    # normalize data using vmin, vmax
    # cax = ax.matshow(matrix, vmin=-1, vmax=1)
    im = ax.imshow(matrix)
    # add a colorbar to a plot.
    fig.colorbar(im)

    xticks = np.arange(0,len(alerts),1)
    yticks = np.arange(0,len(home_ids),1)

    # set x and y tick marks
    ax.set_xticks(xticks)
    ax.set_yticks(yticks)

    # set x and y tick labels
    ax.set_xticklabels(alerts)
    ax.set_yticklabels(home_ids)

    # Turn spines off and create white grid.
    ax.spines[:].set_visible(False)

    ax.set_xticks(np.arange(len(matrix[0])+1)-.5, minor=True)
    ax.set_yticks(np.arange(len(matrix)+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    # for i in range(len(home_ids)):
    #     for j in range(len(alerts)):
    #          text = ax.text(j, i, matrix[i][j], ha="center", va="center", color="w")

    texts = annotate_heatmap(im, valfmt="{x:.1f} t")

    fig.tight_layout()
    plt.show()
    plt.close()


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=("black", "white"),
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A pair of colors.  The first is used for values below a threshold,
        the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts


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
                    plotBasicPeriod(df, path, home_id, "2022-05-24 00:00:00", "2022-05-24 23:59:52", fmt)
                else:
                    if int(file[12]) == 5 and int(file[7:11]) == 2022:
                        home_id = df['home_id'].iloc[0]
                        path = f"plots/{community}/{home_id}"
                        plotBasicPeriod(df, path, home_id, "2022-05-24 00:00:00", "2022-05-24 23:59:52", fmt)
    # Plot an average for a given date for a community
    if AVERAGE_COMMUNITY:
        print("--------------Plotting average--------------")
        # plotAverageCommunity("2022-05-24 00:00:00", "2022-05-24 23:59:52", current_folder, 5, fmt)
        # plotAverageCommunity("2022-05-24 00:00:00", "2022-05-24 23:59:52", current_folder, 11, fmt)


"""
Main function
"""
def main():
    # Manage the data (e.g. resample, etc.)
    if MANAGE_DATA:
        manageData()

    # Check the inconsistency in data
    if INCONSISTENCY:
        checkMistake()

    # Compute and show the information about the alert
    if REACTION:
        computeAlertReaction()

    # Plot 
    if PLOT:
        allPlots()


if __name__ == "__main__":
    main()