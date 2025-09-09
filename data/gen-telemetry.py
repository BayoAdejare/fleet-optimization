#    - Generate synthetic telemetry data
import pandas as pd
import random
import datetime

data = {
    'timestamp': [datetime.datetime.now() for _ in range(100)],
    'vehicle_id': [random.randint(1, 10) for _ in range(100)],
    'engine_temp': [random.uniform(80, 120) for _ in range(100)],
    'fuel_level': [random.uniform(0, 100) for _ in range(100)],
    'tire_pressure': [random.uniform(30, 40) for _ in range(100)]
}
df = pd.DataFrame(data)
df.to_csv('telemetry_data.csv', index=False)