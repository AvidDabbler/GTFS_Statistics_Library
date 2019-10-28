import pandas as pd
import zipfile
import os
import shutil


# C:\Users\Walter\dev\gtfs_conversions\optibus\gtfs.zip

zip_file_loc = input("Where is the gtfs? ")
dir_split = zip_file_loc.split('\\')[:-1]
folder = zip_file_loc[:-4]
output = os.path.join(folder, 'output')
export = os.path.join(output, 'final.csv')


if os.path.exists(folder):
    shutil.rmtree(folder)
    print(f"Deleted {folder}")

os.mkdir(folder)
print(f'Created {folder}')

if os.path.exists(output):
    shutil.rmtree(output)
    print(f"Deleted {output}")
os.mkdir(output)

with zipfile.ZipFile(zip_file_loc, 'r') as myzip:
    myzip.extractall(folder)
    print(f'Unzipped all files into {folder}')

stop_times = pd.read_csv(os.path.join(folder, 'stop_times.txt'))
routes = pd.read_csv(os.path.join(folder, 'routes.txt'))
trips = pd.read_csv(os.path.join(folder, 'trips.txt'))

stop_times_trip = pd.merge(stop_times,trips, left_on='trip_id', right_on='trip_id')
st_trp_routes = pd.merge(stop_times_trip, routes, left_on='route_id', right_on='route_id')

groups = ['trip_id', 'route_id', 'route_short_name']
fmax = ['stop_sequence']

max_group = st_trp_routes.groupby(groups).apply(lambda s: s.loc[s.stop_sequence.idxmax(), fmax])
min_group = st_trp_routes.groupby(groups).apply(lambda s: s.loc[s.stop_sequence.idxmin(), fmax])

max_min_group = pd.merge(max_group, min_group, left_on=groups, right_on=groups)

max_fields = ['trip_id', 'route_id', 'route_short_name', 'stop_sequence_x']
st_trp_routes_ids = ['trip_id', 'route_id', 'route_short_name', 'stop_sequence']
min_fields = ['trip_id', 'route_id', 'route_short_name', 'stop_sequence_y']

max_stopid = pd.merge(max_min_group, st_trp_routes, left_on=max_fields, right_on=st_trp_routes_ids)
min_stopid= pd.merge(max_stopid, st_trp_routes, left_on=min_fields, right_on=st_trp_routes_ids)

fields = ['stop_sequence_x', 'stop_sequence_y', 'departure_time_x',  'stop_sequence_x', 'pickup_type_x', 'drop_off_type_x', 'timepoint_x', 'service_id_x', 'trip_headsign_x', 'direction_id_x', 'block_id_x', 'shape_id_x', 'agency_id_x', 'route_long_name_x', 'route_desc_x', 'route_type_x', 'route_url_x', 'route_color_x', 'route_text_color_x', 'route_sort_order_x', 'min_headway_minutes_x', 'arrival_time_y', 'stop_sequence_y', 'pickup_type_y', 'drop_off_type_y', 'timepoint_y', 'service_id_y', 'trip_headsign_y', 'direction_id_y', 'block_id_y', 'shape_id_y', 'agency_id_y', 'route_long_name_y', 'route_desc_y', 'route_type_y', 'route_url_y', 'route_color_y', 'route_text_color_y', 'route_sort_order_y', 'min_headway_minutes_y']

final = min_stopid.drop(fields, axis=1)

final2 = final.rename(columns={'arrival_time_x': 'Arrival Time',
                              'stop_id_x': 'Destination Stop ID',
                              'departure_time_y': 'Departure Time',
                              'stop_id_y': 'Origin Stop ID',
                              'trip_id': 'Trip ID',
                              'route_id': 'Route ID',
                              'route_short_name': 'Sign (route short name)'})
print('Columns renamed')

final3 = final2[['Trip ID', 'Route ID', 'Sign (route short name)', 'Origin Stop ID', 'Destination Stop ID', 'Departure Time', 'Arrival Time']]

final3.to_csv(export)
print(final3)
print('fin')