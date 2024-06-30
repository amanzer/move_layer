"""
Method to be run by a spawned process to create a matrix of trajectories from a MobilityDB database.
"""


from pymeos.db.psycopg import MobilityDB
from pymeos import *
import psutil
import os
import numpy as np
import time 
from shapely.geometry import Point

def create_matrix( result_queue, begin_frame, end_frame, time_delta_size, extent, timestamps, connection_parameters, granularity_enum, srid, ids_str):
    try:
        logs=""
        pid = os.getpid()
        logs += (f"New process pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}") 
        p_start = timestamps[begin_frame]
        p_end = timestamps[end_frame]
        start_date = timestamps[0]
        x_min,y_min, x_max, y_max = extent
        logs = ""
        
        # Part 1 : Fetch Tpoints from MobilityDB database
        connection_params = {
            "host": connection_parameters["host"],
            "port": connection_parameters["port"],
            "dbname": connection_parameters["dbname"],
            "user": connection_parameters["user"],
            "password": connection_parameters["password"]
        }
        table_name = connection_parameters["table_name"]
        id_column_name = connection_parameters["id_column_name"]
        tpoint_column_name = connection_parameters["tpoint_column_name"]

        connection = MobilityDB.connect(**connection_params)    
        cursor = connection.cursor()
    
        if granularity_enum.value["name"] == "SECOND": 
            time_value = 1 * granularity_enum.value["steps"]
        elif granularity_enum.value["name"] == "MINUTE":
            time_value = 60 * granularity_enum.value["steps"]

        query = f"""WITH trajectories as (
                SELECT 
                    atStbox(
                        a.{tpoint_column_name}::tgeompoint,
                        stbox(
                            ST_MakeEnvelope(
                                {x_min}, {y_min}, -- xmin, ymin
                                {x_max}, {y_max}, -- xmax, ymax
                                {srid} -- SRID
                            ),
                            tstzspan('[{p_start}, {p_end}]')
                        )
                    ) as trajectory
                FROM public.{table_name} as a 
                WHERE a.{id_column_name} in ({ids_str})),

                resampled as (

                SELECT tsample(traj.trajectory, INTERVAL '{granularity_enum.value["steps"]} {granularity_enum.value["name"]}', TIMESTAMP '{start_date}')  AS resampled_trajectory
                    FROM 
                        trajectories as traj)
            
                SELECT
                        EXTRACT(EPOCH FROM (startTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS start_index ,
                        EXTRACT(EPOCH FROM (endTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS end_index,
                        rs.resampled_trajectory
                FROM resampled as rs ;"""

        cursor.execute(query)
        # logs += f"query : {query}\n"
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        logs += f"Number of rows : {len(rows)}\n"
        now_matrix =time.time()
        empty_point_wkt = Point().wkt  # "POINT EMPTY"
        matrix = np.full((len(rows), time_delta_size), empty_point_wkt, dtype=object)
        
        
        rows_to_remove = []
        for i in range(len(rows)):
            if rows[i][2] is not None:
                try:
                    traj_resampled = rows[i][2]

                    start_index = rows[i][0] - begin_frame
                    end_index = rows[i][1] - begin_frame
                    values = np.array([point.wkt for point in traj_resampled.values()])
                    matrix[i, start_index:end_index+1] = values
            
                except Exception as e:
                    logs += f"Error at row {i} : {e}\n"
                    break
            else:
                rows_to_remove.append(i)
        
        matrix = np.delete(matrix, rows_to_remove, axis=0) 

        logs += f"Matrix generation time : {time.time() - now_matrix}\n"
        logs += f"Matrix shape : {matrix.shape}\n"
        logs += f"Number of non empty points : {np.count_nonzero(matrix != 'POINT EMPTY')}\n"

        result_queue.put(0)
        result_queue.put(matrix)
        result_queue.put(logs)
    except Exception as e:
        result_queue.put(1)
        result_queue.put(e)
        result_queue.put(logs)
        return False