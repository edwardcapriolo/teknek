This interface will manage Teknek. It should be able to do a few things at minimum.

Primative Auth with Usename password
View Configures plans as JSON (text box)
Allow editing of plans as JSON (text box)
Provide a debug window that can connect to Kafka topics, to both add data and view results
Provide a CLI like tryredis to be able to build Plans

Interface to create kafka topics and partition settings

In the middle
interface to add jar files containing Operators and Feeds
groovy compiler to build Operators and Feeds on the Fly

Advanced for later
Draw topologies and place components feed and operators on grid
Click on node in grid, connect to it and show last N tuples in and out
Heat map with counters of messages flowing
connect to Workers by JMX
connect to Feeds and operators and insert data directly (debug/troubleshooting)
