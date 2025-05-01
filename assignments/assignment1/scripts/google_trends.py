from pytrends.request import TrendReq
import pandas as pd
import matplotlib.pyplot as plt
import math

def get_trends(keywords, timeframe='today 5-y', geo='US'):
  for i in range(10):
    try:
      pytrends = TrendReq(hl='en-US', tz=360)
      pytrends.build_payload(keywords, cat=0, timeframe=timeframe, geo=geo, gprop='')
      data = pytrends.interest_over_time()
      break
    except:
      if i == 9:
        print('Failed to fetch data')
        return None
      continue

  return data

def get_regional_analysis(keywords, timeframe, geo):
  for i in range(10):
    try:
      pytrends = TrendReq(hl='en-US', tz=360)
      pytrends.build_payload(keywords, cat=0, timeframe=timeframe, geo=geo, gprop='')
      data = pytrends.interest_by_region()
      break
    except:
      if i == 9:
        print('Failed to fetch data')
        return None
      continue

  return data

def save_to_csv(data,data_regional, filename):
  data.to_csv(filename)
  data_regional.to_csv(filename.split('.')[0] + '_regional.csv')

def clean_data(input_file,input_file_regional,output_file,output_file_regional):
  data = pd.read_csv(input_file) # read the interest evolution time
  data_regional = pd.read_csv(input_file_regional) # read the regional interest data
  data = data.drop(columns=['isPartial']) # drop the isPartial column
  data = data.dropna() # drop rows with NaN values
  data_regional = data_regional.dropna() # drop rows with NaN values
  data_regional = data_regional.loc[~(data_regional.drop(columns=['geoName'])==0).all(axis=1)] # drop rows with all 0 values
  data.to_csv(output_file) # save the cleaned data
  data_regional.to_csv(output_file_regional) # save the cleaned regional data

def summarize_data(filename, filename_regional):
  data = pd.read_csv(filename, index_col=0) # read the cleaned interest evolution data
  data_regional = pd.read_csv(filename_regional,index_col=0) # read the cleaned regional interest data

  # calculate the average interest for each keyword
  avg_interest = data.drop(columns=['date'])
  avg_interest = avg_interest.mean()
  avg_interest = avg_interest.to_frame().T
  keywords = avg_interest.columns.values.tolist()
  avg_interests = avg_interest.values.tolist()[0]

  # Plot a bar graph of the average interest
  plt.bar(keywords, avg_interests)
  plt.title('Average interest in keywords')
  plt.xlabel('Keywords')
  plt.ylabel('Interest')
  # Save the plot
  plt.savefig('datasets/figures/average_interest.png')
  # Plot a line graph of the interest evolution
  data.plot(title='Interest evolution in keywords', xlabel='Date', ylabel='Interest')
  # Save the plot
  plt.savefig('datasets/figures/interest_evolution.png')
  print('Average interest in keywords:')
  for i in range(len(keywords)):
    print(keywords[i], str(round(avg_interests[i], 2)))

  # Plot a bar graph of the regional interest (only the top 20 regions)
  # Sort the data by average interest across keywords
  data_regional = data_regional.set_index('geoName')
  

  # Plot the data
  n, m = optimal_grid(len(keywords))
  fig,ax = plt.subplots(n,m,figsize=(10,10))
  for i in range(len(keywords)):
    data_regional = data_regional.sort_values(by=keywords[i], ascending=False)
    data_regional = data_regional.head(20)
    data_regional.plot.bar(y=keywords[i], ax=ax[i//m,i%m], title=keywords[i])
    print('Top 5 regions with the highest interest in', keywords[i])
    for j in range(5):
      print(data_regional.index[j])
  # Save the plot
  plt.tight_layout()
  plt.savefig('datasets/figures/regional_interest.png')

def optimal_grid(N):
  n = int(math.sqrt(N))  # Number of rows
  remaining = N - (n * n)  # Elements left to fit
  m = n + math.ceil(remaining / n)  # Expand columns to fit remaining elements
  return n, m




if __name__ == '__main__':
    keywords = ['remote work', 'work from home','remote job', 'hybrid work']
    data = get_trends(keywords,geo='',timeframe='today 12-m')
    data_regional = get_regional_analysis(keywords,geo='',timeframe='today 12-m')
    if data is not None and data_regional is not None:
      save_to_csv(data,data_regional,'datasets/raw/pytrends.csv')
    else:
      exit()
      
    clean_data('datasets/raw/pytrends.csv','datasets/raw/pytrends_regional.csv','datasets/processed/cleaned_pytrends.csv','datasets/processed/cleaned_pytrends_regional.csv')

    summarize_data('datasets/processed/cleaned_pytrends.csv','datasets/processed/cleaned_pytrends_regional.csv')