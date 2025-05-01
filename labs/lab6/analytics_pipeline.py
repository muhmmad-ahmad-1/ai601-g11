import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger

@task
def fetch_data(data_path):
    logger = get_run_logger()
    df = pd.read_csv(data_path)
    logger.info(f"Data shape: {df.shape}")
    return df

@task
def data_validation(df):
    logger = get_run_logger()
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()

    return df_clean

@task
def data_transformation(df):
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
    
    return df

@task
def data_analytics(df):
    summary = df.describe()
    summary.to_csv("data/analytics_summary.csv")

@task
def visualize(df):
    if "sales" in df.columns:
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("data/sales_histogram.png")
        plt.close()


@flow
def pipeline():
    logger = get_run_logger()
    df_path = "analytics_data.csv"

    # Step 1: Fetch Data
    logger.info("Reading data...")
    df = fetch_data(df_path)

    # Step 2: Validate Data
    logger.info("Validating data...")
    df_clean = data_validation(df)

    # Step 3: Transform Data
    logger.info("Transforming data...")
    df_clean = data_transformation(df_clean)

    # Step 4: Generate Analytics Report
    logger.info("Generating analytics report...")
    data_analytics(df_clean)

    # Step 5: Create a Histogram for Sales Distribution
    visualize(df_clean)


# def main():
    
#     # Step 1: Fetch Data
#     print("Reading data...")
#     # Assume a dataset with sales figures and other fields is provided.
#     df = pd.read_csv("data/analytics_data.csv")
#     print(f"Data shape: {df.shape}")

#     # Step 2: Validate Data
#     print("Validating data...")
#     missing_values = df.isnull().sum()
#     print("Missing values:\n", missing_values)
#     # For simplicity, drop any rows with missing values
#     df_clean = df.dropna()

#     # Step 3: Transform Data
#     print("Transforming data...")
#     # For example, if there is a "sales" column, create a normalized version.
#     if "sales" in df_clean.columns:
#         df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()

#     # Step 4: Generate Analytics Report
#     print("Generating analytics report...")
#     summary = df_clean.describe()
#     summary.to_csv("data/analytics_summary.csv")
#     print("Summary statistics saved to data/analytics_summary.csv")

#     # Step 5: Create a Histogram for Sales Distribution
#     if "sales" in df_clean.columns:
#         plt.hist(df_clean["sales"], bins=20)
#         plt.title("Sales Distribution")
#         plt.xlabel("Sales")
#         plt.ylabel("Frequency")
#         plt.savefig("data/sales_histogram.png")
#         plt.close()
#         print("Sales histogram saved to data/sales_histogram.png")

#     print("Analytics pipeline completed.")

if __name__ == "__main__":
    pipeline()
