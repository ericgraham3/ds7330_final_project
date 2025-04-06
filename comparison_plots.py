import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

print("\nGenerating plots")

try:
    mysql_times = pd.read_csv("mysql_execution_times.csv")
    mysql_avg = mysql_times['execution times'].mean()
except:
    print("No MySQL Execution Time file. Please run a set of iterations in MySQL")
try:
    pg_times = pd.read_csv("pg_execution_times.csv")
    pg_avg = pg_times['execution times'].mean()
except:
    print("No PostgreSQL Execution Time file. Please run a set of iterations in PostgreSQL tab")
try:
    mongo_times = pd.read_csv("mongo_execution_times.csv")
    mongo_avg = mongo_times['execution times'].mean()
except:
    print("No MongoDB Execution Time file. Please run a set of iterations in MongoDB tab")

try:
    averages = [mysql_avg, pg_avg, mongo_avg]
except:
    print("Don't have all averages yet. Please run each database tab at least once")

def plot_execution_times(df, dbtype, filename="execution_plot.png"):
    """
    Creates a scatter plot of execution times and saves it as an image.

    Parameters:
    - execution_times (list of float): List of execution times.
    - filename (str): Name of the file to save the plot (default: "execution_plot.png").
    """
    try:
        # Calculate average
        avg_time = df['execution times'].mean()

        # Plot
        plt.figure(figsize=(8, 5))
        plt.scatter(df['run'], df['execution times'], color='blue', label='Execution Time')
        plt.axhline(y=avg_time, color='red', linestyle='--', label='Average')
        plt.text(x=max(df['run']) + 0.1, y=avg_time,
                 s=f'Avg: {avg_time:.4f} s', color='red', va='center')

        # Labels and title
        plt.xlabel('Run')
        plt.ylabel('Execution Time (s)')
        plt.title(f'{dbtype} Execution Time per Run')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        # Save and show
        plt.savefig(filename)
    except:
        print(f'No such file for {dbtype}')

def plot_side_by_side_averages(avgs, labels=None, filename="averages_comparison.png"):
    try:
        if labels is None:
            labels = ['Database 1', 'Database 2', 'Database 3']

        x = np.arange(len(avgs))

        plt.figure(figsize=(6,5))
        bars = plt.bar(x, avgs, color=['skyblue', 'salmon', 'lightgreen'])

        # Add value labels above bars
        for i, bar in enumerate(bars):
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.01, f'{yval:.4f} s',
                     ha='center', va='bottom')

        plt.xticks(x, labels)
        plt.ylabel('Average Execution Time (s)')
        plt.title('Comparison of Average Execution Times')
        plt.tight_layout()

        plt.savefig(filename)
    except:
        print("Don't have all averages yet. Please run each database tab at least once")

# scatterplots for each database
try:
    plot_execution_times(mysql_times, "MySQL", "mysql_execution_plot.png")
except:
    print("No MySQL Execution Time file. Please run a set of iterations in MySQL")

try:
    plot_execution_times(pg_times, "PostgreSQL", "pg_execution_plot.png")
except:
    print("No PostgreSQL Execution Time file. Please run a set of iterations in PostgreSQL tab")

try:
    plot_execution_times(mongo_times, "MongoDB", "mongo_execution_plot.png")
except:
    print("No MongoDB Execution Time file. Please run a set of iterations in MongoDB tab")

# barplot to compare them side-by-side
try:
    plot_side_by_side_averages(averages, ['MySQL', 'PostgreSQL', 'MongoDB'])
except:
    print("Don't have all averages yet. Please run each database tab at least once")