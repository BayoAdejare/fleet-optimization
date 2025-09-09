import datetime
import random
import pandas as pd

#    - Simulate GPS data with random coordinates:
data = {
    'timestamp': [datetime.datetime.now() for _ in range(100)],
    'vehicle_id': [random.randint(1, 10) for _ in range(100)],
    'latitude': [random.uniform(-90, 90) for _ in range(100)],
    'longitude': [random.uniform(-180, 180) for _ in range(100)]
}
df = pd.DataFrame(data)
df.to_csv('gps_data.csv', index=False)