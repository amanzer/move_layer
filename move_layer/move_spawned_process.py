"""
Method to be run by a spawned process to create a matrix of trajectories from a MobilityDB database.
"""

from .move_worker_process import worker_fnc
from pymeos.db.psycopg import MobilityDB
from pymeos import *
import psutil
import os
import numpy as np
import time 
from shapely.geometry import Point
import multiprocessing


def create_matrix( result_queue, begin_frame, end_frame, time_delta_size, extent, timestamps, connection_parameters, granularity_enum, srid, ids_str):
    try:
        logs=""
        pid = os.getpid()
        logs += (f"pid : {pid} create_matrix |  CPU affinity : {psutil.Process(pid).cpu_affinity()} \n") 
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
        empty_point_wkb = Point().wkb  # "POINT EMPTY"
        dtype = np.dtype(f'V{25}')
        matrix = np.full((len(rows), time_delta_size), empty_point_wkb, dtype=dtype)
        
        
        # rows_to_remove = []
        for i in range(len(rows)):
            if rows[i][2] is not None:
                try:
                    traj_resampled = rows[i][2]

                    start_index = rows[i][0] - begin_frame
                    end_index = rows[i][1] - begin_frame
                    values = np.array([point.wkb for point in traj_resampled.values()])
                    matrix[i, start_index:end_index+1] = values
            
                except Exception as e:
                    logs += f"Error at row {i} : {e}\n"
                    break
            # else:
            #     rows_to_remove.append(i)
        
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
    




def create_matrix_multi_cores( result_queue, begin_frame, end_frame, time_delta_size, extent, timestamps, connection_parameters, granularity_enum, srid, total_ids):
    try:
        logs=""

        # cpus = [[x, x+1] for x in range(2, 12, 2)]
        cpu_count = psutil.cpu_count()
        half_cpu_count = cpu_count // 2
        cpus = [i for i in range(half_cpu_count, cpu_count)]

        pid = os.getpid()
        psutil.Process(pid).cpu_affinity(cpus) # Assign the process to the last 4 cores

        logs += (f"pid : {pid} create_matrix |  CPU affinity : {psutil.Process(pid).cpu_affinity()} \n") 
        empty_point_wkb = Point().wkb  # "POINT EMPTY"
      

        

        num_workers = len(cpus)-1
        # logs += f"Number of workers : {num_workers}\n"
        ids_per_process = int(np.ceil(len(total_ids)) / num_workers)
        
       

        # Distributing the ids to the workers
        ids_sub_list = [total_ids[i:i+ids_per_process] for i in range(0, len(total_ids), ids_per_process)] 


        pool = multiprocessing.Pool(num_workers)

        
        logs += f"{cpus[0]}"
        worker_args = [(ids_sub_list[i], begin_frame, end_frame, time_delta_size, connection_parameters, granularity_enum, extent, srid, timestamps, cpus[i]) for i in range(len(ids_sub_list))]
        
        
        # pool.map(process_chunk2, worker_args)
        results = pool.map(worker_fnc, worker_args)
        
        dtype = np.dtype(f'V{25}')
        matrix = np.full((len(total_ids), time_delta_size), empty_point_wkb, dtype=dtype)

        # check if one of results first argument is 1, if so, raise an error
        for i, (status, chunk_matrix, worker_logs) in enumerate(results):
            if status == 1:
                raise ValueError(worker_logs)

            start_idx = i * ids_per_process
            end_idx = start_idx + len(chunk_matrix)
            matrix[start_idx:end_idx, :] = chunk_matrix
            logs += worker_logs


        result_queue.put(0)
        result_queue.put(matrix)
        result_queue.put(logs)
        return True

    except Exception as e:
        result_queue.put(1)
        result_queue.put(e)
        result_queue.put(logs)
        return False

    finally:
        pool.close()
        pool.join()

        