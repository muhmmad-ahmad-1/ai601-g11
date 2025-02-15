import pandas as pd
import matplotlib.pyplot as plt
import math

def summarize_data(filepath):
    df = pd.read_csv(filepath)
    df = df.drop(columns="Employee_ID")

    print("\n===== Dataset Summary =====")
    print(f"Total Rows: {df.shape[0]}")
    print(f"Total Columns: {df.shape[1]}")
    print("\nBasic Statistics for Numerical Columns:")
    n = len(df.columns)
    print(df.iloc[:,:n//2-1].describe())  
    print(df.iloc[:,n//2-1:].describe()) 
    print("\nUnique Values in Categorical Columns:")
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        print(f"- {col}: {df[col].nunique()} unique values")

    pie_columns = list(df.columns)
    bar_columns = ['Age','Years_of_Experience','Hours_Worked_Per_Week','Number_of_Virtual_Meetings','Productivity_Change']
    for b in bar_columns:
        pie_columns.remove(b)
    n,m = optimal_grid(len(pie_columns))
    fig, axes = plt.subplots(n,m, figsize=(m * 4, n * 4))
    axes = axes.flatten()

    for i,col in enumerate(pie_columns):
        df[col].value_counts().plot.pie(ax=axes[i], autopct='%1.1f%%', title=col)
        axes[i].set_ylabel("")
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.subplots_adjust(hspace=0.3, wspace=0.3) 
    fig.set_constrained_layout(True)
    plt.savefig('datasets/figures/public_dataset_pie.png')

    

    n,m = optimal_grid(len(bar_columns))
    fig, axes = plt.subplots(n,m, figsize=(8*n, 4*m))
    axes = axes.flatten()

    for i,col in enumerate(bar_columns):
        
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col].plot(kind='hist', bins=6, ax=axes[i], title=col, edgecolor='black', rwidth=0.8)
        else:
            info = df[col].value_counts()
            info.plot(kind='bar', ax=axes[i], title=col)
            axes[i].set_xlabel("")
            
    
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.subplots_adjust(hspace=0.3, wspace=0.3) 
    fig.set_constrained_layout(True)
    plt.savefig('datasets/figures/public_dataset_bar.png')

def optimal_grid(N):
  n = int(math.sqrt(N))  # Number of rows
  remaining = N - (n * n)  # Elements left to fit
  m = n + math.ceil(remaining / n)  # Expand columns to fit remaining elements
  return n, m

if __name__ == "__main__":
    summarize_data('datasets/raw/public_dataset.csv')