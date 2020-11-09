# GTFS Statistics Library
Using pandas and shapely to manipulate a gtfs for usage in GIS data vizualizations

## GTFS()
Use the initial GTFS class to load in a zipped up version of a GTFS and loads up all of the text files as DataFrames for later use. 

The following files from the GTFS are loaded in automatically and can be accessed after the initial GTFS call by:
- agency
- calendar
- calendar_dates
- fare_attributes
- fare_rules
- routes
- shapes
- stops
- stop_times
- transfers
- trips

```
gtfs = GTFS(zip)
stops = gtfs.stops
routes = gtfs.routes
```

## times_stops_trips()
Merges together the stop_times, trips, and routes text files as a DataFrame to easily work with the stops, times and routes data. 

This library is set up for feeds that are setup for non-frequency feeds so it works best if each trip in the feed specifies an inidivdual trip.

## trip_stats()
The trip_stats function is used for statistics for each of the trips. Right now this function with calculate out the following statistics for each trip after finding the first and last stop for each trip:
- max_stop_sequence - index for the last stop in a trip
- min_stop_sequence - index for the first stop in a trip
- max_arrival_time - time at which the last stop arrives
- max_departure_time - time at which the last stop departs
- min_arrival_time - time at which the first stop arrives
- min_departure_time - time at which the first stop departs
- max_stop_id - stop id of the last stop in a trip
- min_stop_id - stop id of the first stop in a trip
- trip_time - total time in minutes that it takes for that trip to run



## route_stats()
The route_stats function is used to get specific statistics about a particular route and it groups together all of the trips with in that feed to come up with these stats. This is still in development, but will eventually work by creating a better way to calculate out real life frequencies for reference. 
