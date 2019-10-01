import pandas as pd
import shapely

# DEFINED ALL OF THE TEXT FILES IN GTFS
stops = pd.read_csv(r'gtfs/stops.txt')
stop_times = pd.read_csv(r'gtfs/stop_times.txt')
trips = pd.read_csv(r'gtfs/trips.txt')
routes = pd.read_csv(r'gtfs/routes.txt')
shapes = pd.read_csv(r'gtfs/shapes.txt')

# JOINING AND EXPORT OF GEOGRAPHIC INFORMATION TO STOP_TIMES.TXT
stop_times_join = pd.merge(stop_times, stops, left_on='stop_id', right_on="stop_id")
print('*** MERGE STOP_TIMES AND STOPS ***')
stop_times_join = stop_times_join.loc[:,~stop_times_join.columns.duplicated()]
stop_times_join.to_csv(r'output/stop_times_loc.csv')
print('*** STOP_TIMES_LOC.CSV CREATED ***')

# TRIP INFORMATION JOINED TO STOP TIMES
super_stops = pd.merge(stop_times_join, trips, on="trip_id")
print('*** MERGE STOP_TIMES AND TRIPS ***')
super_stops = pd.merge(super_stops, routes, on="route_id")
print('*** MERGE STOP_TIMES AND ROUTES ***')
super_stops = super_stops.drop(columns=['wheelchair_boarding', 'wheelchair_accessible'])
super_stops.sort_values(by=['trip_id', 'arrival_time', 'stop_sequence'], inplace=True)

# CALCULATE END OF LINE AND BEGINNING OF LINE
begin = super_stops['stop_sequence'] == 1
super_stops['begin'] = begin
end = super_stops.loc[super_stops.groupby('trip_id').stop_sequence.agg('idxmax')]
end.rename(columns={"stop_sequence": "max_seq"}, inplace=True)
super_stops = pd.merge(super_stops, end[['trip_id','max_seq']], on='trip_id')
super_stops['end'] = super_stops['stop_sequence'] == super_stops['max_seq']


# CLEAN UP AND EXPORT super_stops
super_stops.drop(columns=['max_seq'])
super_stops = super_stops.loc[:,~super_stops.columns.duplicated()]
super_stops.to_csv(r'output/super_stops.csv')
print('*** SUPER_STOPS.CSV CREATED ***')

# CREATE UNIQUE ID FOR SHAPES FOR SUPER_STOPS TO LATCH ON TO
shapes = pd.merge(shapes, trips[['trip_id', 'shape_id']], on='shape_id')
print('*** MERGE SHAPES AND TRIPS ***')
shapes = shapes.loc[:,~shapes.columns.duplicated()]
shapes['unique'] = str(shapes['trip_id']) + '_' + str(shapes['shape_pt_sequence'])

# FILTER SHAPES AND ASSOCIATE TRIPS_JOIN
for t in trips['trip_id']:
    ss = super_stops['trip_id'] == t
    sh = shapes['trip_id'] == t
    for stop in ss['stop_id']:
        stop_loc = stop[['stop_lat','stop_lon']]
        ss_loc = ss[['shape_pt_lat','shape_pt_lon']]
        super_stops['shape_unique'] =

# CREATE A LIST OF TRIP_ID'S **this is already set up in the trips.txt file


# USE THE TRIP_ID'S LIST IN A FOR LOOP TO FILTER AND ASSOCIATE TO STOP_ID
# ASSOCIATE STOP_ID TO SHAPES NODE BY LOOKING AT CLOSEST XY
# IF SHAPES['STOP_ID'] FOR I IS NONE SHAPES['STOP_ID'] I = SHAPES['STOP_ID'] I-1

