
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
            self.id_column_name = connection_parameters["id_column_name"]
            self.tpoint_column_name = connection_parameters["tpoint_column_name"]     
            self.connection = MobilityDB.connect(**connection_params)
            x_min,y_min, x_max, y_max = extent
            self.cursor = self.connection.cursor()
            query = f"""
                                WITH trajectories as (
                                    SELECT 
                                        atStbox(
                                            a.{self.tpoint_column_name}::tgeompoint,
                                            stbox(
                                                ST_MakeEnvelope(
                                                    {x_min}, {y_min}, -- xmin, ymin
                                                    {x_max}, {y_max}, -- xmax, ymax
                                                    {self.srid} -- SRID
                                                )
                                            )
                                        ) as trajectory, a.{self.id_column_name} as id
                                    FROM public.{self.table_name} as a )

                                    SELECT tr.id                            
                                    FROM trajectories as tr where tr.trajectory is not null ;
                                """
            
            self.cursor.execute(query)
    
            self.ids_list = self.cursor.fetchall()
            self.ids_list = self.ids_list[:int(len(self.ids_list)*self.percentage_of_objects)]
            self.objects_count = len(self.ids_list)

            ids_list = [ f"'{id[0]}'"  for id in self.ids_list]
            self.objects_id_str = ', '.join(map(str, ids_list))


        except Exception as e:
            self.log(f"Error in DatabaseConnector init : {e}")


    def get_total_ids(self):
        return self.ids_list
    

    def get_objects_str(self):
        return self.objects_id_str


    def get_objects_count(self):
        return self.objects_count
        

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
    