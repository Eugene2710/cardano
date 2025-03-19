import os
import psycopg2
from dotenv import load_dotenv

"""
Documentation of Demeter's API is relatively lacking in explanation of features and the structure of the data, 
Example of response from this file:
(11620496, <memory at 0x105bd4b80>, 546, 150654533, 145733, 11615097, 11620495, 5616171, 4, datetime.datetime(2025, 3, 17, 14, 13, 44), 0, 10, 2, 'vrf_vk10e789vyjaks8l3q0gqxzhs7mn03xj4tpdtuzl3v7lvc77derxnmqvh4t7d', <memory at 0x105bd4ac0>, 19)
(11620495, <memory at 0x105bd4c40>, 546, 150654531, 145731, 11615096, 11620494, 4557275, 4, datetime.datetime(2025, 3, 17, 14, 13, 42), 0, 10, 2, 'vrf_vk17v5w0aak280g633jdkce0wl8ev89cp9uk5rqca6p769lj8nps56q6t2z9t', <memory at 0x105bd4d00>, 24)

As seen above, there are different columns but no specification of what each data refers to. Hence, we will be using
Blockfrost instead
"""

load_dotenv()

connection = psycopg2.connect(
    host="dbsync-v3.demeter.run",
    port=5432,
    user=os.getenv("DEMETER_USER", ""),
    password=os.getenv("DEMETER_PASSWORD", ""),
    dbname="dbsync-mainnet",
)

cursor = connection.cursor()

cursor.execute(f"SELECT * FROM block ORDER BY id DESC LIMIT 10")

rows = cursor.fetchall()
for row in rows:
    print(row)
