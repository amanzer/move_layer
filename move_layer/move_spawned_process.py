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


def create_matrix( result_queue, begin_frame, end_frame, time_delta_size, start_date, p_start, p_end, connection_parameters, granularity_enum, srid, total_trajectories):
    try:
        logs=""
        pid = os.getpid()
        logs += (f"pid : {pid} create_matrix |  CPU affinity : {psutil.Process(pid).cpu_affinity()} \n") 
       

        logs = ""

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

                    attime({tpoint_column_name}::tgeompoint,
	                        span('{p_start}'::timestamptz, 
                                '{p_end}'::timestamptz, true, true)) as trip
                    FROM public.{table_name} LIMIT {total_trajectories}),


                resampled as (

                SELECT tsample(trip, INTERVAL '{granularity_enum.value["steps"]} {granularity_enum.value["name"]}', TIMESTAMP '{start_date}')  AS resampled_trip
                    FROM 
                        trajectories)
            
                SELECT
                        EXTRACT(EPOCH FROM (startTimestamp(resampled_trip) - '{start_date}'::timestamp))::integer / {time_value} AS start_index ,
                        EXTRACT(EPOCH FROM (endTimestamp(resampled_trip) - '{start_date}'::timestamp))::integer / {time_value} AS end_index,
                        resampled_trip
                FROM resampled;"""

        cursor.execute(query)
        # logs += f"query : {query}\n"
        rows= []
        while True:
            results = cursor.fetchmany(1000)
            if not results:
                break
            rows.extend(results)
        cursor.close()
        connection.close()

        logs += f"Number of rows : {len(rows)}\n"
        now_matrix =time.time()
        empty_point_wkb = Point().wkb  # "POINT EMPTY"
        dtype = object
        matrix = np.full((len(rows), time_delta_size), empty_point_wkb, dtype=dtype)
        
        
   
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


        logs += f"Matrix generation time : {time.time() - now_matrix}\n"
        logs += f"Matrix shape : {matrix.shape}\n"


        result_queue.put(0)
        result_queue.put(matrix)
        result_queue.put(logs)
    except Exception as e:
        result_queue.put(1)
        result_queue.put(f"UKULELELLELLELE {e}")
        result_queue.put(logs)
        return False
    




def create_matrix_multi_cores( result_queue, begin_frame, end_frame, time_delta_size, start_date, p_start, p_end, connection_parameters, granularity_enum, srid, total_trajectories):
    try:
        logs=""

        cpu_count = psutil.cpu_count()
        half_cpu_count = cpu_count // 2
        cpus = [i for i in range(half_cpu_count, cpu_count)]

        pid = os.getpid()
        psutil.Process(pid).cpu_affinity(cpus) # Assign the process to the last 4 cores

        logs += (f"pid : {pid} create_matrix |  CPU affinity : {psutil.Process(pid).cpu_affinity()} \n") 
        empty_point_wkb = Point().wkb  # "POINT EMPTY"
      

        
        num_workers = len(cpus)
        # Distributing the rows to the workers
        rows_per_process = int(np.ceil(total_trajectories / num_workers))
        ids_per_process = [(i, (i+rows_per_process)-1) for i in range(1, total_trajectories, rows_per_process)]

        pool = multiprocessing.Pool(num_workers)

        
        worker_args = [(ids_per_process[i], begin_frame, end_frame, time_delta_size, connection_parameters, granularity_enum, srid, start_date, p_start, p_end , cpus[i]) for i in range(len(ids_per_process))]
        
        results = pool.map(worker_fnc, worker_args)
        
        dtype = object
        matrix = np.full((total_trajectories, time_delta_size), empty_point_wkb, dtype=dtype)


        for i, (status, chunk_matrix, worker_logs) in enumerate(results):
            if status == 1:
                logs += f"Error in worker {i} \n"
                raise ValueError(worker_logs)

            start_idx = i * rows_per_process
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
        result_queue.put(logs + "OOGABOOGA" + f"{ids_per_process} - {cpus}")
        return False

    finally:
        pool.close()
        pool.join()

        