import json
with open('data/launches.json', 'r') as file:
    data = json.load(file)

# Check the first row to determine columns
columns = data[0].keys()
print("Columns:", columns)

# Check the data types of each column
for column in columns:
    data_type = type(data[0][column])
    print(f"Column: {column}, Type: {data_type}")