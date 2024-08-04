
from pymeos.db.psycopg import MobilityDB

from qgis.core import QgsMessageLog
from qgis.core import Qgis

class DatabaseConnector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self, connection_parameters, extent, percentage_of_objects, srid):
        try: 
            connection_params = {
            "host": connection_parameters["host"],
            "port": connection_parameters["port"],
            "dbname": connection_parameters["dbname"],
            "user": connection_parameters["user"],
            "password": connection_parameters["password"],
            }
            self.percentage_of_objects = percentage_of_objects
            self.srid = srid
            self.table_name = connection_parameters["table_name"]
            # self.id_column_name = connection_parameters["id_column_name"]
            self.tpoint_column_name = connection_parameters["tpoint_column_name"]     
            self.connection = MobilityDB.connect(**connection_params)
            # x_min,y_min, x_max, y_max = extent
            self.cursor = self.connection.cursor()


            # query_create_id = f""" ALTER TABLE public.{self.table_name} ADD COLUMN id SERIAL UNIQUE;"""
            # self.cursor.execute(query_create_id)
            # query_create_index = f""" CREATE INDEX idx_id ON public.{self.table_name} (id); """
            # self.cursor.execute(query_create_index)

            query_index = f""" SELECT max(id) FROM public.{self.table_name}; """
            self.cursor.execute(query_index)
            self.objects_count = self.cursor.fetchone()[0]

    


        except Exception as e:
            self.log(f"Error in DatabaseConnector init : {e}")


    def get_total_ids(self):
        return self.ids_list
    

    def get_objects_str(self):
        return self.objects_id_str


    def get_objects_count(self):
        
        return self.objects_count
        

    def get_fields(self, limit):
        """
        Returns the fields of the table.
        """
        try:
            query = f"SELECT startTimestamp({self.tpoint_column_name}), endTimestamp({self.tpoint_column_name}) FROM public.{self.table_name} ORDER BY id LIMIT {limit};"
            self.cursor.execute(query)
            results= []
            while True:
                rows = self.cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)
            return results
        except Exception as e:
            self.log(f"Error in get_fields : {e}")

    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            # self.log(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            self.log(f"Error in get_min_timestamp : {e}")


    def get_max_timestamp(self):
        """
        Returns the max timestamp of the tpoints columns.

        """
        try:
            self.cursor.execute(f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            self.log(f"Error in get_max_timestamp : {e}")


    def close(self):
        """
        Close the connection to the MobilityDB database.
        """
        self.cursor.close()
        self.connection.close()

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)
    