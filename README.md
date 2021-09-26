# data-engineering-udacity

#### Introduction
This project creates a database using postgresql and data from a startup, Sparkify. The database includes multiple tables for future analytical purpose.
The design of the database schema was based Star schemas. In the schemas, there is one fact table, songplays, and four dimension tables, including users, songs, artists, and time. With query among these tables, the startup can easily perform data analysis.

#### Implementation
The programme was designed to run in a Docker container but can also be revised to run without Docker.
example command to create a container to run Cassandra
```bash
docker run --name cassandra -p 127.0.0.1:9042:9042 --network some-network -d cassandra
```
