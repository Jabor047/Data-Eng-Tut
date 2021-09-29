import psycopg2
from config import p_pass
database = "Sensor_DW"

# establishing the connection
conn = psycopg2.connect(
user = 'postgres', password = p_pass, host = '127.0.0.1', port = '5432'
)
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()

# Preparing query to create a database
sql = f'''CREATE database {database}'''

# Creating a database
cursor.execute(sql)
print("Database created successfully........")

# Closing the connection
conn.close()

