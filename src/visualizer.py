import pandas as pd
import matplotlib.pyplot as plt

def plot_days_since_access(df: pd.DataFrame):
    """
    Plots a histogram of the days since last access for tables that have been accessed.

    Args:
        df (pd.DataFrame): The dataframe containing 'days_since_last_access' and 'usage_status'.
    """
    # Filter for accessed tables
    accessed = df[df['usage_status'] == 'Accessed'].copy()

    # Ensure numerical type
    accessed['days_since_last_access'] = pd.to_numeric(accessed['days_since_last_access'], errors='coerce')
    accessed = accessed.dropna(subset=['days_since_last_access'])

    if accessed.empty:
        print("No accessed tables found to plot histogram.")
        return

    plt.figure(figsize=(10, 6))
    plt.hist(accessed['days_since_last_access'], bins=30, color='#1f77b4', edgecolor='black', alpha=0.7)
    plt.title('Distribution of Days Since Last Access')
    plt.xlabel('Days Since Last Access')
    plt.ylabel('Number of Tables')
    plt.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    plt.show()

def plot_unused_vs_used_counts(df: pd.DataFrame):
    """
    Plots a pie chart showing the proportion of tables that have been accessed vs. never accessed.

    Args:
        df (pd.DataFrame): The dataframe containing 'usage_status'.
    """
    if 'usage_status' not in df.columns:
        print("Column 'usage_status' not found in DataFrame.")
        return

    counts = df['usage_status'].value_counts()

    if counts.empty:
        print("No data to plot.")
        return

    plt.figure(figsize=(8, 8))
    # Colors: Never Accessed (Red-ish), Accessed (Green-ish)
    colors = ['#ff9999', '#66b3ff']

    # Note: The order of colors depends on the order of counts.
    # We can be more specific but simple is fine for now.

    counts.plot(kind='pie', autopct='%1.1f%%', startangle=140, colors=colors)
    plt.title('Proportion of Accessed vs. Never Accessed Tables')
    plt.ylabel('') # Hide y-label
    plt.tight_layout()
    plt.show()
