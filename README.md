# CERITAS DATA LAYER

ceritas_data_layer.py contains the class Ceritas_Database.
This class starts up a connection to our Ceritas database, given correct credentials.
There are a number of functions to help with querying the database.

To create a database instance, initialize a class like this:
```
from ceritas_data_layer import Ceritas_Database

db_instance = Ceritas_Database(database, user, password, host)
```
