from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from geopy.distance import geodesic
from loaders import ingest_json_file_into_elastic_index
import pandas as pd
import numpy as np
import ast
import smopy
from gurobipy import Model,GRB
import json
import matplotlib.pyplot as plt

#part 1
#Initialize Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
index_name = 'car_sharing' #elastisearch index name
file_path = 'request_data_repair.json'

# check if index already exists, delete it if it does to ensure clean state.
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print("Existing 'car_sharing' index deleted")

# Create the index with settings
index_settings = {
    "settings": {"number_of_shards": 3},
    "mappings": {
        "properties": {
            'origin_datetime': {'type': 'date', "format": "yyyy-MM-dd HH:mm:ss"},
            'destination_datetime': {'type': 'date', "format": "yyyy-MM-dd HH:mm:ss"},
            'origin_location': {'type': 'geo_point'},
            'destination_location': {'type': 'geo_point'},
            'request_nr': {'type': 'integer'},
            'weekend': {'type': 'integer'}
        }
    }
}
#creaate an index with previously defined settings and mapping
es.indices.create(index=index_name, body=index_settings)
ingest_json_file_into_elastic_index(file_path, es, index_name, buffer_size=500)
es.indices.refresh(index=index_name) #refresh the index to make all recent changes searchable
print(f"Initial number of entries = {es.count(index=index_name)['count']}")


# Calculate distance bewtween two geo-points
def calculate_distance(origin, destination):
    results= geodesic(origin, destination)
    return results

# Calculate time difference in hours
def calculate_time_difference(start_time, end_time):
    time_delta = end_time - start_time
    time_difference= time_delta.total_seconds()/3600 #Number of second in an hour
    return time_difference
#A function that swap lon and lat in a location list
def swiping_lon_lat(wrong_location):
    right_location = [wrong_location[1], wrong_location[0]]
    return right_location

#process data in batches to calculate destance , time, speed and then update the index
batch_size = 10000
batch = []
query = {
    "query": {
        "bool": {
            "must": {
                "match_all": {}
            }
        }
    }
}
for hit in helpers.scan(es, index=index_name, query=query):
    origin_location = hit['_source']['origin_location']
    destination_location = hit['_source']['destination_location']
    origin_datetime = datetime.strptime(hit['_source']['origin_datetime'], "%Y-%m-%d %H:%M:%S")
    destination_datetime = datetime.strptime(hit['_source']['destination_datetime'], "%Y-%m-%d %H:%M:%S")
    # Swipe longs and lats
    origin_location = swiping_lon_lat(origin_location)
    destination_location = swiping_lon_lat(destination_location)
    # Calculate distance and time difference
    distance_km = calculate_distance(origin_location, destination_location).km
    time_hours = calculate_time_difference(origin_datetime, destination_datetime)
    speed_kmh = distance_km / time_hours if time_hours > 0 else 0

    # Determine if the record should be flagged for deletion based on conditions
    if origin_location != destination_location and origin_datetime != destination_datetime and 5 <= speed_kmh <= 60:
        delete_flag = 0
    else:
        delete_flag = 1
    update_request = {
        "_op_type": "update",
        "_index": index_name,
        "_id": hit['_id'],
        "doc": {
            "distance_km": distance_km,
            "time_hours": time_hours,
            "speed_kmh": speed_kmh,
            "delete": delete_flag
        }
    }
    batch.append(update_request)
    if len(batch) >= batch_size:
        helpers.bulk(es, batch)
        batch = []
        print("sent to elasticsearch")

if batch:  # Ensure the last batch is also processed
    helpers.bulk(es, batch)

# query to delete all element which have delete = 1
query = {
  "query": {
    "term": {
      "delete": {
        "value": 1
      }
    }
  }
}
es.delete_by_query(index=index_name, body=query, wait_for_completion=True)
es.indices.refresh(index=index_name)
print(f"Number of entries after cleaning = {es.count(index=index_name)['count']}")


#part2
# Analyzing demand over time
query = {
    "size": 0,
    "aggs": {
        "demand_over_time": {
            "date_histogram": {
                "field": "origin_datetime",
                "calendar_interval": "day"
            }
        }
    }
}
response = es.search(index=index_name, body=query)
buckets = response['aggregations']['demand_over_time']['buckets']
dates = [bucket['key_as_string'] for bucket in buckets]
counts = [bucket['doc_count'] for bucket in buckets]
#create a dataFrame
df = pd.DataFrame({'Date': pd.to_datetime(dates), 'Demand': counts})
df.set_index('Date', inplace=True)

plt.figure(figsize=(14, 7))
df['Demand'].plot()
plt.title('Daily Demand Over the Past Year')
plt.xlabel('Date')
plt.ylabel('Demand')
plt.grid(True)
plt.show()


#part 3
#query to analyze hourly demand on typical working day
hourly_demand_query= {
    "query": {
        "bool": {
            "must" :[
                {"term" :{"weekend":0}} #filter for weekday data
            ]
        }
    },
    "size":0,
    "aggs": {
        "hourly_demand": {
            "date_histogram":{
                "field": "origin_datetime",
                "calendar_interval" : "hour",
                "min_doc_count" : 1
            },
        }
    }
}

response = es.search( index= index_name, body= hourly_demand_query)
buckets = response["aggregations"]["hourly_demand"]["buckets"]
hours = [datetime.strptime(bucket['key_as_string'], "%Y-%m-%d %H:%M:%S").hour for bucket in buckets]
counts=[bucket['doc_count'] for bucket in buckets]

# Create and prepare the DataFrame for plotting
hourly_df = pd.DataFrame({'Hour': hours, 'Demand': counts})
hourly_stats = hourly_df.groupby('Hour')['Demand'].agg(['mean', 'std', 'min', 'max']).reset_index()
overall_mean_demand = hourly_stats['mean'].mean()

# Plotting hourly demand using a bar chart
plt.figure(figsize=(14, 7))
plt.bar(hourly_stats.index, hourly_stats['mean'], color='skyblue', label='Average Demand (Mean)')

# Min and Max values as points on top of each bar
plt.scatter(hourly_stats['Hour'], hourly_stats['min'], color='red', marker='o', s=50, label='Min Demand', zorder=5)
plt.scatter(hourly_stats['Hour'], hourly_stats['max'], color='blue', marker='o', s=50, label='Max Demand', zorder=5)

plt.axhline(y=overall_mean_demand, color='green', linestyle='--', label='Overall Mean Demand', zorder=4)

plt.title('Hourly Demand on Typical Working Days (Bar Chart)')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Requests')
plt.legend()
plt.grid(True)
plt.xticks(hourly_stats.index)  # Ensure all hours are labeled
plt.show()

#3b
# Query to get daily demand for working days
daily_demand_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"weekend": 0}}  # Ensure data is from weekdays
            ]
        }
    },
    "size": 0,
    "aggs": {
        "daily_demand": {
            "date_histogram": {
                "field": "origin_datetime",
                "calendar_interval": "day",
                "min_doc_count": 1
            }
        }
    }
}
daily_demand_response = es.search(index=index_name, body=daily_demand_query)
#extract the daily demand counts
daily_demand = [bucket['doc_count'] for bucket in daily_demand_response['aggregations']['daily_demand']['buckets']]
#calculate the average daily demand
avg_daily_demand= np.mean(daily_demand)
#Increase by 30% to account the unsatisfied demand
new_avg_daily_demand= round(avg_daily_demand * 1.3,2)
print(f"Average daily demand of satisfied : {avg_daily_demand:.2f} ")
print(f"Average daily demand for unsatisfied demand : {new_avg_daily_demand}")
# Plotting
plt.figure(figsize=(8, 6))
plt.bar(['Original', 'Adjusted'], [avg_daily_demand, new_avg_daily_demand], color=['skyblue', 'orange'])
plt.title('Average Daily Demand Comparison')
plt.xlabel('Type of Demand')
plt.ylabel('Average Daily Demand')
plt.show()

#3c
# Define the query to bring raw data
raw_data_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"weekend": 0}}  # filter for weekdays only
            ]
        }
    },
    "size": 10000,
    "_source": ["origin_datetime", "request_nr", "weekend", "origin_location", "destination_datetime", "destination_location", "date", "hour"]  # Specify the fields to retrieve}
# Adjust size based on the expected number of records
}
# Execute raw data query
response = es.search(index=index_name, body=raw_data_query)
results = [hit['_source'] for hit in response['hits']['hits']]
df = pd.DataFrame(results)
df['origin_datetime'] = pd.to_datetime(df['origin_datetime'])
df['weekday'] = df['origin_datetime'].dt.weekday

# Sampling based on adjusted demand
working_days_df = df[df['weekday'].isin(range(5))]
sampled_requests = working_days_df.sample(n=int(np.ceil(new_avg_daily_demand)), replace=True)

# Visualization and output
print("Original Dataset Size:", len(df))
print("Sample Size:", len(sampled_requests))
sampled_requests.to_excel('sampled_requestss.xlsx', index=False, engine='openpyxl')
print(sampled_requests)
plt.figure(figsize=(12, 6))
plt.hist(sampled_requests['origin_datetime'].dt.hour, bins=range(24), color='skyblue', edgecolor='black', label='Sampled Requests')
plt.title('Distribution of Adjusted Sampled Requests')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Requests')
plt.xticks(range(24))
plt.legend()
plt.grid(True)
plt.show()


#part 4



# Load data
sampled_requests = pd.read_excel('sampled_requestss.xlsx')  # Ensure this file has the proper structure and location

# origin location is a string representation of a list, convert it to an actual list
sampled_requests['origin_location'] = sampled_requests['origin_location'].apply(ast.literal_eval)
# Correct coordinate order for requests
sampled_requests['swiped_location'] = sampled_requests['origin_location'].apply(swiping_lon_lat)

# Separate the swiped location into two new columns 'lon' and 'lat'
sampled_requests[['lat', 'lon']] = pd.DataFrame(sampled_requests['swiped_location'].tolist(), index=sampled_requests.index)
# Load car data with all lines reversed
print(sampled_requests)
with open("car_locations_repair.json", 'r') as file:
    car_locations = [json.loads(line) for line in file]

# Convert the corrected data to a DataFrame
cars_df = pd.DataFrame(car_locations)

cars_df['lat'] = cars_df['start_location'].apply(lambda x: x[0]) #by apply lambda it seperates the column lon and lat
cars_df['lon'] = cars_df['start_location'].apply(lambda x: x[1])
print(cars_df.head())
print(f"The sample size for optimization is: {len(sampled_requests)}")
#determine if a car and a request are compatible based on the walking distance
def is_compatible(car_position, request_position, max_walking_distance=400):
    return geodesic(car_position, request_position).meters <= max_walking_distance


for request_index, request_row in sampled_requests.iterrows():
    request_position = (request_row['lat'], request_row['lon'])
    for car_index, car_row in cars_df.iterrows():
        car_position = (car_row['start_location'][0], car_row['start_location'][1])
        distance = geodesic(car_position, request_position).meters
        if distance < 400:  # Print only reasonable distances for inspection
            print(f"Distance from Car {car_index} to Request {request_index}: {distance} meters")

speed_km_per_hour = 60 #km/h
speed_km_per_min= speed_km_per_hour *1000/ 60 #convert the km/h to m/min
revenue_per_minute = 0.19

def calculate_profit(distance):
    minutes = distance / speed_km_per_min
    return minutes * revenue_per_minute
# Define the model
m = Model("CarSharingAssignment")

# Decision variables
x = m.addVars(len(cars_df), len(sampled_requests), vtype=GRB.BINARY, name="assign")

# Compatibility and profit matrices
compatibility = {}
profit = {}
# Initialize profit dictionary entries for all car-request pairs to 0
for c in range(len(cars_df)):
    for j in range(len(sampled_requests)):
        profit[c, j] = 0  # Default profit is zero if not compatible
for j, request in sampled_requests.iterrows():
    request_position = (request['lat'], request['lon'])
    request_destination = swiping_lon_lat(ast.literal_eval(request['destination_location']))  # Assuming destination format adjustment

    for c, car in cars_df.iterrows():
        car_position = (car['lat'], car['lon'])
        compatibility[c, j] = is_compatible(car_position, request_position)
        if compatibility[c, j]:
            distance_to_destination = calculate_distance(car_position, request_destination).meters
            profit[c, j] = calculate_profit(distance_to_destination)
            m.addConstr(x[c, j] <= compatibility[c, j], name=f"compat_{c}_{j}")

# Objective: Maximize total profit from compatible car request
m.setObjective(sum(profit[c, j] * x[c, j] for c in range(len(cars_df)) for j in range(len(sampled_requests))), GRB.MAXIMIZE)

# Constraints
# Each request is assigned to at most one car
for j in range(len(sampled_requests)):
    m.addConstr(sum(x[c, j] for c in range(len(cars_df))) <= 1, name=f"oneCar_{j}")

# Each car has at most one request assigned
for c in range(len(cars_df)):
    m.addConstr(sum(x[c, j] for j in range(len(sampled_requests))) <= 1, name=f"oneRequest_{c}")

m.optimize()

# Check the optimization status and print the results
if m.status == GRB.OPTIMAL:
    print("Optimal solution found.")
    for c in range(len(cars_df)):
        for j in range(len(sampled_requests)):
            if (c, j) in x and x[c, j].X > 0.5:  # Checking if the decision variable is 1
                profit = calculate_profit(geodesic((cars_df.loc[c, 'lat'], cars_df.loc[c, 'lon']),
                                                   sampled_requests.loc[j, ['lat', 'lon']]).meters)
                print(f"Request {j} assigned to Car {c} with Profit: {profit}")
else:
    print(f"No optimal solution found. Status code: {m.status}")


#4d
#Handling data that exist in multiple formats
#this function converts a string represeantion of a list ot tuple into actual list or tuple
def safe_eval(x):
    try:
        # Attempt to evaluate string to tuple
        return ast.literal_eval(x)
    except (ValueError, SyntaxError):
        # Return the value as is if it's already a tuple/list
        return x

# Apply the safe_eval function to extract coordinates
# Adjust the lambda functions to handle data that might already be in the correct format
sampled_requests['origin_lat'] = sampled_requests['origin_location'].apply(lambda x: safe_eval(x)[1])
sampled_requests['origin_lon'] = sampled_requests['origin_location'].apply(lambda x: safe_eval(x)[0])
sampled_requests['destination_lat'] = sampled_requests['destination_location'].apply(lambda x: safe_eval(x)[1])
sampled_requests['destination_lon'] = sampled_requests['destination_location'].apply(lambda x: safe_eval(x)[0])



# Define the bounds of the map based on locations
lat_min = min(sampled_requests['origin_lat'].min(), sampled_requests['destination_lat'].min())
lat_max = max(sampled_requests['origin_lat'].max(), sampled_requests['destination_lat'].max())
lon_min = min(sampled_requests['origin_lon'].min(), sampled_requests['destination_lon'].min())
lon_max = max(sampled_requests['origin_lon'].max(), sampled_requests['destination_lon'].max())

# Create the map
map_area = smopy.Map((lat_min, lon_min, lat_max, lon_max), z=12)
map_img = map_area.to_pil()

fig, ax = plt.subplots(figsize=(20, 24))
ax.imshow(map_img)
plt.axis('off')

# Function to plot points
def plot_point(ax, x, y, color='blue', size=40, marker='o', popup_text=''):
    ax.scatter(x, y, color=color, s=size, marker=marker, edgecolors='black', zorder=10)
    if popup_text:
        plt.text(x, y, popup_text, fontsize=8)

# Plotting locations
for index, row in cars_df.iterrows():
    x, y = map_area.to_pixels(row['lat'], row['lon'])
    plot_point(ax, x, y, color='blue', size=45, marker='^')  # Triangle marker for cars

for index, row in sampled_requests.iterrows():
    orig_x, orig_y = map_area.to_pixels(row['origin_lat'], row['origin_lon'])
    dest_x, dest_y = map_area.to_pixels(row['destination_lat'], row['destination_lon'])
    plot_point(ax, orig_x, orig_y, color='green', size=5, marker='o')  # Circle marker for origins
    plot_point(ax, dest_x, dest_y, color='red', size=5, marker='s')  # Square marker for destinations

# Add legends and labels
plt.title('Car Sharing Assignments')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.legend(['Cars'])

# Show the map
plt.show()

#part 5
# Define parameters

max_walking_distance = 400 #meters
max_relocation_distance = 3000  # meters
relocation_cost = 25  # euros


# Initialize Gurobi model
m = Model("CarSharingAssignment")

# Binary decision variable for assign car to requests
x = m.addVars(len(cars_df), len(sampled_requests), vtype=GRB.BINARY, name="assign")
# Binary decision variable for relocation
y = m.addVars(len(cars_df), len(sampled_requests), vtype=GRB.BINARY, name="relocate")
#Calculate profit for potential assignments of cars to requests
profit = {}
for c, car in cars_df.iterrows():
    for j, request in sampled_requests.iterrows():
        distance = geodesic((car['lat'], car['lon']), (request['lat'], request['lon'])).meters
        if distance <= max_walking_distance:
            profit[c, j] = (distance / speed_km_per_min) * revenue_per_minute #profit if within walking distance
# Objective: Maximize total adjusted profit (profit minus potential relocation costs)
m.setObjective(sum(profit[c, j] * x[c, j] - relocation_cost * y[c, j] for c, j in profit), GRB.MAXIMIZE)
# Constraints
# Each request can only be assigned to one car, and within walking distance
for j in range(len(sampled_requests)):
    m.addConstr(sum(x[c, j] for c in range(len(cars_df)) if (c, j) in profit) <= 1, name=f"assign_request_{j}")

# Each car can only serve at most one request
for c in range(len(cars_df)):
    m.addConstr(sum(x[c, j] for j in range(len(sampled_requests)) if (c, j) in profit) <= 1, name=f"serve_once_{c}")

# Relocation constraints: relocate only if assigned and needed, and within the relocation distance
for c in range(len(cars_df)):
    for j in range(len(sampled_requests)):
        if (c, j) in profit and geodesic(
                (cars_df.at[c, 'lat'], cars_df.at[c, 'lon']),
                (sampled_requests.at[j, 'lat'], sampled_requests.at[j, 'lon'])).meters <= max_relocation_distance:
            m.addConstr(y[c, j] <= x[c, j], name=f"relocation_needed_{c}_{j}")

# Optimize the model
m.optimize()

# Check the optimization status and print the results
if m.status == GRB.OPTIMAL:
    total_profit_without_relocation = m.objVal - m.getObjective().getValue()
    total_profit_with_relocation = m.objVal
    if total_profit_with_relocation > total_profit_without_relocation:
        print("Hiring the relocation service is beneficial.")
    else:
        print("Hiring the relocation service is not beneficial.")
else:
    print("No optimal solution found.")



#part 6


# a range of potential fees to evaluate
rental_fees = np.arange(0.19, 0.31, 0.01)

# Lists to store results for plotting later
profits_with_relocation = []
profits_without_relocation = []

# Loop through each rental fee to evaluate its impact on profit
for fee in rental_fees:
    m = Model("CarSharingProfitSimulation")

    # Decision variables
    x = m.addVars(len(cars_df), len(sampled_requests), vtype=GRB.BINARY, name="assign")
    y = m.addVars(len(cars_df), len(sampled_requests), vtype=GRB.BINARY, name="relocate")

    # profit calculation based on the current rental fee
    profit = {}
    for c, car in cars_df.iterrows():
        for j, request in sampled_requests.iterrows():
            distance = geodesic((car['lat'], car['lon']), (request['lat'], request['lon'])).meters
            if distance <= max_walking_distance:
                profit[c, j] = (distance / speed_km_per_min) * fee  # Use new rental fee

    # Objective function to maximize profit considering relocation costs
    m.setObjective(sum(profit.get((c, j), 0) * x[c, j] - relocation_cost * y[c, j] for c, j in profit), GRB.MAXIMIZE)

    # Constraints
    #Each request is assigned to at most one car within walking distance
    for j in range(len(sampled_requests)):
        m.addConstr(sum(x[c, j] for c in range(len(cars_df)) if (c, j) in profit) <= 1)
    #Each car serves at most one request
    for c in range(len(cars_df)):
        m.addConstr(sum(x[c, j] for j in range(len(sampled_requests)) if (c, j) in profit) <= 1)
    # Control car relocation based on maximum relocation distance
    for c in range(len(cars_df)):
        for j in range(len(sampled_requests)):
            if (c, j) in profit and geodesic((cars_df.at[c, 'lat'], cars_df.at[c, 'lon']),
                                             (sampled_requests.at[j, 'lat'],
                                              sampled_requests.at[j, 'lon'])).meters <= max_relocation_distance:
                m.addConstr(y[c, j] <= x[c, j])

    # Optimize the model
    m.optimize()

    # Store results
    if m.status == GRB.OPTIMAL:
        total_profit = m.objVal
        profits_with_relocation.append(total_profit - sum(relocation_cost * y[c, j].X for c, j in profit))
        profits_without_relocation.append(total_profit)

# Plotting
plt.figure(figsize=(10, 5))
plt.plot(rental_fees, profits_with_relocation, label='With Relocation', marker='o')
plt.plot(rental_fees, profits_without_relocation, label='Without Relocation', marker='x')
plt.xlabel('Rental Fee (€ per started minute)')
plt.ylabel('Profit (€)')
plt.title('Impact of Rental Fee Increase on Profits')
plt.legend()
plt.grid(True)
plt.show()