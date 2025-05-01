import requests
import csv

URL = "https://archive-api.open-meteo.com/v1/archive?latitude=31.5204&longitude=74.3587&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&timezone=auto"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    ## TODO: complete the code, the output should be data in json format
    return response.json()

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately
        headers = list(data["hourly"].keys())
        data_writer = csv.writer(file)
        data_writer.writerow(headers)
        daily_data = []
        for header in headers:
            daily_data.append(data["hourly"][header])
        daily_data = zip(*daily_data)
        data_writer.writerows(daily_data)
        
        return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    ### TODO: complete rest of the code
    data = []
    with open(input_file, "r", newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for i, row in enumerate(csv_reader):
            if i != 0:
                try:
                    row[1], row[2], row[3] = float(row[1]), float(row[2]), float(row[3])
                except:
                    continue
                if (row[1] >= 0 and row[1] <= 60) and (row[2] >= 0 and row[2] <= 80) and (row[3] >= 3 and row[3] <= 150):
                    data.append(row)
            else:
                data.append(row)
    
    with open(output_file, "w", newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data)
    print("Cleaned data saved to", output_file)

### Part 4. Aggregation Operation 
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)  # Convert CSV data to list

        # Ensure we have data
        if not data:
            print("No data available to summarize.")
            return

        # Extract values from columns
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2]]
        wind_speeds = [float(row[3]) for row in data if row[3]]

        # Compute statistics
        ### TODO: complete rest of the code by computing the below mentioned metrics
        total_records = len(temperatures)
        avg_temp = sum(temperatures)/total_records
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values)/total_records
        avg_wind_speed = sum(wind_speeds)/total_records
        # Print summary
        print("📊 Weather Data Summary 📊")
        print(f"Total Records: {total_records}")
        print(f"🌡️ Average Temperature: {avg_temp:.2f}°C")
        print(f"🔥 Max Temperature: {max_temp:.2f}°C")
        print(f"❄️ Min Temperature: {min_temp:.2f}°C")
        print(f"💧 Average Humidity: {avg_humidity:.1f}%")
        print(f"💨 Average Wind Speed: {avg_wind_speed:.2f} m/s")


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        print("Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        

