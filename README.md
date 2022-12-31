# Spotify-API-Project

This project extracts track level data from the Spotify API into two separate dataframes, transforms and loads the data into two separate tables in Google BigQuery through a single python script, spotify_extraction.py. For the "TRACKS" table in GBQ, yesterdays data is appended to the table (todays date is not used because the data is not present until the following day). The "POPULARITY" table is loaded separately because the popularity metric is dynamic and should be updated instead of appended. The python script is run daily through a CRON job on my local machine.

In GBQ a couple of SQL statements are run manually to check 
  1. The correlation between the length of a song and its popularity
  2. The distribution of releases by day of the week
