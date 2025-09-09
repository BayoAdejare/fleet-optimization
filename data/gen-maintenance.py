import random
import datetime
import pandas as pd

#    - Generate synthetic maintenance logs:
data = {
    'vehicle_id': [random.randint(1, 10) for _ in range(20)],
    'maintenance_type': [random.choice(['oil_change', 'tire_rotation', 'brake_check']) for _ in range(20)],
    'date': [datetime.datetime.now() for _ in range(20)],
    'cost': [random.uniform(50, 200) for _ in range(20)]
}
df = pd.DataFrame(data)
df.to_csv('maintenance_logs.csv', index=False)