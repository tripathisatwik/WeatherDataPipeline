import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://api.open-meteo.com/v1/forecast?latitude=27.7172&longitude=85.3240&current_weather=true"

def fetch_weather_data():
    response = requests.get(URL,timeout=30)
    if response.status_code == 200:
        data = response.json()

        event = {
            "timestamp" : datetime.utcnow().isoformat(),
            "data" : data  
        }

        Path("data_lake").mkdir(exist_ok=True)
        filename = f"data_lake/wather_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"

        with open(filename,"w") as f:
            json.dump(event, f)

        print("File Created and Data Loaded Sucessfuly at:", filename)

    else:
        
        print(f"Failed to fetch data: {response.status_code}")

if __name__ == "__main__":
    fetch_weather_data()
