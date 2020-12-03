import pandas as pd
import zipfile
import os
import shutil


class GTFS:
    def __init__(self, zip):
        self.zip = zip
        self.dir_split = self.zip.split('\\')[:-1]
        self.root = '\\'.join(self.dir_split)
        self.folder = self.zip[:-4]
        self.output = os.path.join(self.folder, 'output')
        self.export = os.path.join(self.output, 'final.csv')

        if os.path.exists(self.folder):
            shutil.rmtree(self.folder)

        os.mkdir(self.folder)

        if os.path.exists(self.output):
            shutil.rmtree(self.output)
            print(f"Deleted {self.output}")

        os.mkdir(self.output)

        def fix_times(row):
            time = row.split(':')
            hour = int(time[0])
            list = time[1:]

            if int(hour) >= 24:
                hour = (hour - 24)
                list.insert(0, str(hour))
                t = ':'.join(list)
                dt = pd.to_datetime(f'01-02-2020 {t}', format='%m-%d-%Y %H:%M:%S')
            else:
                list.insert(0, str(hour))
                t = ':'.join(list)
                dt = pd.to_datetime(f'01-01-2020 {t}', format='%m-%d-%Y %H:%M:%S')

            return dt
            

        with zipfile.ZipFile(self.zip, 'r') as myzip:
            myzip.extractall(self.folder)
        if os.path.exists(os.path.join(self.folder, 'agency.txt')):
            self.agency = pd.read_csv(os.path.join(self.folder, 'agency.txt'))        
        if os.path.exists(os.path.join(self.folder, 'calendar.txt')):
            self.calendar = pd.read_csv(os.path.join(self.folder, 'calendar.txt'))      
        if os.path.exists(os.path.join(self.folder, 'calendar_dates.txt')):
            self.calendar_dates = pd.read_csv(os.path.join(self.folder, 'calendar_dates.txt'))
        if os.path.exists(os.path.join(self.folder, 'fare_attributes.txt')):
            self.fare_attributes = pd.read_csv(os.path.join(self.folder, 'fare_attributes.txt'))
        if os.path.exists(os.path.join(self.folder, 'fare_rules.txt')):
            self.fare_rules = pd.read_csv(os.path.join(self.folder, 'fare_rules.txt'))
        if os.path.exists(os.path.join(self.folder, 'routes.txt')):
            self.routes = pd.read_csv(os.path.join(self.folder, 'routes.txt'))
        if os.path.exists(os.path.join(self.folder, 'shapes.txt')):
            self.shapes = pd.read_csv(os.path.join(self.folder, 'shapes.txt'))
        if os.path.exists(os.path.join(self.folder, 'stops.txt')):
            self.stops = pd.read_csv(os.path.join(self.folder, 'stops.txt'))
        if os.path.exists(os.path.join(self.folder, 'stop_times.txt')):
            self.stop_times = pd.read_csv(os.path.join(self.folder, 'stop_times.txt'))
            self.stop_times['arrival_time'] = self.stop_times['arrival_time'].apply(lambda s: fix_times(s))
            self.stop_times['departure_time'] = self.stop_times['departure_time'].apply(lambda s: fix_times(s))
        if os.path.exists(os.path.join(self.folder, 'transfers.txt')):
            self.transfers = pd.read_csv(os.path.join(self.folder, 'transfers.txt'))
        if os.path.exists(os.path.join(self.folder, 'trips.txt')):
            self.trips = pd.read_csv(os.path.join(self.folder, 'trips.txt'))

        shutil.rmtree(self.folder)

    def max_min_rename(self, df):
        fields_obj = {}

        for column in df.columns.values.tolist():
            if column.endswith('_x'):
                fields_obj[column] = f'max_{column[:-2]}'
            if column.endswith('_y'):
                fields_obj[column] = f'min_{column[:-2]}'
        return df.rename(columns=fields_obj)

    def rename_list(self, df, list):
        obj = {}
        for field in list:
            obj[field] = f'max_{field}'
        df = df.rename(obj)
        return df

    def times_stops_trips(self):

        # merge stops_times, trips, and routes
        try:
            # merge stop_times with trips
            self.stop_times_trip = pd.merge(self.stop_times,self.trips, left_on='trip_id', right_on='trip_id')

            # merge stop_times_trip with routes for full dataframe table
            self.stop_times_trip_route = pd.merge(self.stop_times_trip, self.routes, left_on='route_id', right_on='route_id')
        except:
            print('Error merging datasets. Possibly issue with field names.')
        
        return self.stop_times_trip_route

    def trip_stats(self):
        self.stop_times_trip_route = self.times_stops_trips()

        # group by trips and routes & find last and first stops by trip and route
        try:
            fields = ['trip_id', 'route_id', 'route_short_name', 'route_long_name', 'stop_id', 'stop_sequence']
            groups = ['trip_id', 'route_id', 'route_short_name', 'route_long_name']
            fmax = ['stop_sequence']
            
            # find the max stop_sequece id's and add a boolean end column
            max_group = self.stop_times_trip_route.groupby(groups).apply(lambda s: s.loc[s.stop_sequence.idxmax(), fmax])

            # find the min stop_sequece id's and add a boolean start column
            min_group = self.stop_times_trip_route.groupby(groups).apply(lambda s: s.loc[s.stop_sequence.idxmin(), fmax])
                    
        except:
            print('Issue with group by calculations')

        mm_group = self.max_min_rename(pd.merge(max_group, min_group, left_on=groups, right_on=groups))

        print(mm_group)

        max_min_group = mm_group.loc[:,~mm_group.columns.duplicated()]

        min_ids = ['trip_id', 'route_id', 'route_short_name', 'min_stop_sequence']
        max_ids = ['trip_id', 'route_id', 'route_short_name', 'max_stop_sequence']
        st_trp_routes_ids = ['trip_id', 'route_id', 'route_short_name', 'stop_sequence']
        update_fields = ['stop_id', 'arrival_time', 'departure_time']
        drop_fields = ['pickup_type', 'drop_off_type', 'timepoint', 'shape_dist_traveled', 'trip_headsign', 'direction_id', 'wheelchair_accessible', 'route_type', 'route_color', 'route_text_color']
        max_drop = ['service_id', 'block_id', 'shape_id']

        # join min max table to stop times trips route table
        max_stopid = pd.merge(max_min_group, self.stop_times_trip_route, left_on=max_ids, right_on=st_trp_routes_ids)
        max_stopid = self.rename_list(max_stopid, update_fields).drop(drop_fields, axis=1).drop(max_drop, axis=1)

        stats_df = pd.merge(max_stopid, self.stop_times_trip_route, left_on=min_ids, right_on=st_trp_routes_ids)
        stats_df = self.rename_list(stats_df, update_fields).drop(drop_fields, axis=1)

        stats_df = self.max_min_rename(stats_df)

        stats_df['trip_time'] = (pd.to_datetime(stats_df['max_departure_time'], format='%H:%M:%S') - pd.to_datetime(stats_df['min_departure_time'], format='%H:%M:%S'))
        stats_df['trip_time'] = stats_df['trip_time'].apply(lambda s: s.total_seconds()/60)

        print(type(stats_df['trip_time'][1]))

        final = stats_df.loc[:,~stats_df.columns.duplicated()]

        return final
    
    def route_stats(self):
        # return max min frequency
        # total trips
        # total shapes
        # max min trip time
        
        trips = self.trip_stats()

        r_fields = ['route_id', 'route_short_name', ''] 
        trips['trip_count'] = 1
        routes = trips[r_fields].groupby(r_fields).count()

        return routes
