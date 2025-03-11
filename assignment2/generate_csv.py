import csv
import random
import datetime
import string
import os
from collections import defaultdict

def generate_session_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

def random_datetime(date):
    return datetime.datetime.combine(date, datetime.time(hour=random.randint(0, 23), 
                                                        minute=random.randint(0, 59), 
                                                        second=random.randint(0, 59)))

def generate_user_activity_logs(filename, start_date, days, min_rows=25, max_rows=30):
    actions = ['play', 'pause', 'skip', 'forward']
    devices = ['mobile', 'desktop', 'tablet']
    regions = ['US', 'EU', 'APAC']
    user_ids = range(100, 201)
    content_ids = range(1000, 1011)
    
    # Dictionary to store logs by date
    daily_logs = defaultdict(list)
    
    # Generate all logs first
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['user_id', 'content_id', 'action', 'timestamp', 'device', 'region', 'session_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for day_offset in range(days):
            date = start_date + datetime.timedelta(days=day_offset)
            num_rows = random.randint(min_rows, max_rows)
            session_mapping = {}
            
            for _ in range(num_rows):
                user_id = random.choice(user_ids)
                content_id = random.choice(content_ids)
                action = random.choice(actions)
                timestamp = random_datetime(date)
                device = random.choice(devices)
                region = random.choice(regions)
                
                if user_id not in session_mapping or random.random() < 0.3:
                    session_mapping[user_id] = generate_session_id()
                session_id = session_mapping[user_id]
                
                log_entry = {
                    'user_id': user_id,
                    'content_id': content_id,
                    'action': action,
                    'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'device': device,
                    'region': region,
                    'session_id': session_id
                }
                
                writer.writerow(log_entry)
                # Store in daily_logs using date as key
                date_str = timestamp.strftime('%Y-%m-%d')
                daily_logs[date_str].append(log_entry)
    
    # Create daily folders and separate log files
    for date_str, logs in daily_logs.items():
        # Create folder if it doesn't exist
        folder_name = date_str
        os.makedirs(folder_name, exist_ok=True)
        
        # Write logs for this specific day
        daily_filename = os.path.join(folder_name, 'user_activity_logs.csv')
        with open(daily_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(logs)
        
        print(f"Created {daily_filename} with {len(logs)} entries")

def generate_content_metadata(filename):
    content_titles = ["Summer Vibes", "Rock Anthem", "Daily News", "Jazz Classics", "Tech Talk", 
                     "Indie Dreams", "Hip Hop Hustle", "Classical Moods", "Workout Mix", 
                     "Acoustic Serenity", "History Deep Dive"]
    categories = ['Pop', 'Rock', 'Podcast', 'News', 'Jazz', 'Indie', 'Hip Hop', 'Classical', 'Electronic', 'Acoustic']
    artists = ["DJ Alpha", "The Beats", "Anchor FM", "Smooth Sax", "Guru Mike", "The Wanderers", 
              "MC Flow", "Orchestra 21", "DJ Pulse", "Strum & Co.", "Professor X"]
    content_ids = range(1000, 1011)
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['content_id', 'title', 'category', 'length', 'artist']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for content_id in content_ids:
            writer.writerow({
                'content_id': content_id,
                'title': random.choice(content_titles),
                'category': random.choice(categories),
                'length': random.randint(120, 1200),
                'artist': random.choice(artists)
            })

# Generate CSV files
start_date = datetime.date(2024, 3, 1)
generate_user_activity_logs('user_activity_logs.csv', start_date, days=28)
generate_content_metadata('content_metadata.csv')

print("Main CSV files generated: user_activity_logs.csv, content_metadata.csv")
print("Daily log files have been created in respective YYYY-MM-DD folders")