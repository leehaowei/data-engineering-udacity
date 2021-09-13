# data-engineering-udacity

#### Goals and brief intro
This project create a database using postgresql and data from a startup, Sparkify. The database includes multiple tables for analytical purpose.
The design of the database schema was based Star schemas. In the schemas, there is one fact table, songplays and four dimension tables, including users, songs, artists, and time. With query among these tables, the startup can perform analysis using the structured database.

#### Implementation and sample query
##### Implementation
the programme was designed to run in a Docker container but can also be revised to run without Docker

##### sample query
sparkifydb=# select * from users limit 3;
 user_id | first_name | last_name | gender | level 
---------+------------+-----------+--------+-------
      75 | Joseph     | Gutierrez | M      | free
      44 | Aleena     | Kirby     | F      | paid
       6 | Cecilia    | Owens     | F      | free
