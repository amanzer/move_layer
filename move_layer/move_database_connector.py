
from pymeos.db.psycopg import MobilityDB

from qgis.core import QgsMessageLog
from qgis.core import Qgis



class Database_connector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self, connection_parameters):
        try: 
            connection_params = {
            "host": connection_parameters["host"],
            "port": connection_parameters["port"],
            "dbname": connection_parameters["dbname"],
            "user": connection_parameters["user"],
            "password": connection_parameters["password"],
            }
   

            self.table_name = connection_parameters["table_name"]
            self.id_column_name = connection_parameters["id_column_name"]
            self.tpoint_column_name = connection_parameters["tpoint_column_name"]   
            self.tfloat_columns = connection_parameters["tfloat_columns"]  
            self.connection = MobilityDB.connect(**connection_params)
     
            self.cursor = self.connection.cursor()
            query = f"""
                    SELECT {self.id_column_name} FROM public.{self.table_name} ;
                    """
            
            self.cursor.execute(query)
                                
            self.ids_list = self.cursor.fetchall()
            # self.ids_list = self.ids_list[:int(len(self.ids_list)*PERCENTAGE_OF_OBJECTS)]
            self.objects_count = len(self.ids_list)

        except Exception as e:
            self.log(e)

    def get_objects_ids(self):
        return self.ids_list


    def get_objects_count(self):
        return self.objects_count
        
    def get_tfloat_columns(self):
        return self.tfloat_columns

    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            # log(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            self.log(e)


    def get_max_timestamp(self):
        """
        Returns the max timestamp of the tpoints columns.

        """
        try:
            self.cursor.execute(f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            self.log(e)


    def get_tgeompoints(self, p_start, p_end, extent, srid, n_objects):

        x_min,y_min, x_max, y_max = extent
        ids = self.ids_list[:n_objects]
        ids_list = [ f"'{id[0]}'"  for id in ids]
        objects_id_str = ', '.join(map(str, ids_list))
        # Part 1 : Fetch Tpoints from MobilityDB database
      
        tfloat_query = ""
        tfloat_query2 = ""
        for tfloat_column in self.tfloat_columns:
            tfloat_query += f", a.{tfloat_column}"
            tfloat_query2 += f", rs.{tfloat_column}"

        query = f"""WITH trajectories as (
                SELECT 
                    atStbox(
                        a.{self.tpoint_column_name}::tgeompoint,
                        stbox(
                            ST_MakeEnvelope(
                                {x_min}, {y_min}, -- xmin, ymin
                                {x_max}, {y_max}, -- xmax, ymax
                                {srid} -- SRID
                            ),
                            tstzspan('[{p_start}, {p_end}]')
                        )
                    ) as trajectory {tfloat_query}
                FROM public.{self.table_name} as a 
                WHERE a.{self.id_column_name} in ({objects_id_str}))
            
                SELECT
                        rs.trajectory {tfloat_query2}
                FROM trajectories as rs ;"""

        self.cursor.execute(query)
        self.log(f"query : {query}\n")
        return self.cursor.fetchall()


    def close(self):
        """
        Close the connection to the MobilityDB database.
        """
        self.cursor.close()
        self.connection.close()

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'Move', level=Qgis.Info)

