import numpy as np
from shapely.geometry import Point
import psutil
import os
import traceback

def worker_fnc(args):
    try:
        from pymeos.db.psycopg import MobilityDB
        ids, begin_frame, end_frame, time_delta_size, connection_parameters, granularity_enum, extent, srid, timestamps, cpus = args
        empty_point_wkb = Point().wkb
        start_date = timestamps[0]

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


        pid = os.getpid()
        psutil.Process(pid).cpu_affinity([cpus])
            

        ids_list_str = [ f"'{id[0]}'"  for id in ids]
        ids_str = ', '.join(map(str, ids_list_str))


        p_start = timestamps[begin_frame]
        p_end = timestamps[end_frame]
        start_date = timestamps[0]
        x_min,y_min, x_max, y_max = extent
        
        connection = MobilityDB.connect(**connection_params)    
        cursor = connection.cursor()

        if granularity_enum.value["name"] == "SECOND": # TODO : handle granularity of different time steps(5 seconds etc)
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

     
        dtype = np.dtype(f'V{25}')
        chunk_matrix = np.full((len(rows), time_delta_size), empty_point_wkb, dtype=dtype)
        logs = f"pid : {pid} assigned to cpu : {cpus} \n"

        for i in range(len(rows)):
            if rows[i][2] is not None:
                try:
                    traj_resampled = rows[i][2]

                    start_index = rows[i][0] - begin_frame
                    end_index = rows[i][1] - begin_frame
                    values = np.array([point.wkb for point in traj_resampled.values()])
                    chunk_matrix[i, start_index:end_index+1] = values
                except:
                    raise ValueError(f"Error in asignation : {start_index} - {end_index} | begin_frame : {begin_frame} | end_frame : {end_frame} | pstart {p_start} | pend {p_end} | start_date {start_date} | {len(rows[i][2].values())} \n {query}")
                
            

        return 0, chunk_matrix, logs
    except Exception as e:
        err = traceback.format_exc()
        return 1, None, f"Error in worker: {e} - {err} \n"


    
 