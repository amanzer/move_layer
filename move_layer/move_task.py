import psycopg2

from qgis.core import QgsTask


import multiprocessing
import os
import psutil
from qgis.core import QgsMessageLog
from qgis.core import Qgis



class Matrix_generation_thread(QgsTask):
    """
    This thread creates next time delta's the matrix containing the positions for all objects to show. 
    """
    def __init__(self, description,project_title, beg_frame, end_frame, start_date, p_start, p_end, time_delta_size, connection_params, granularity_enum, srid, total_tpoints,  create_matrix_fnc, finished_fnc, failed_fnc):
        super(Matrix_generation_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title

        self.begin_frame = beg_frame
        self.end_frame = end_frame

        self.start_date = start_date
        self.p_start = p_start
        self.p_end = p_end
        self.time_delta_size = time_delta_size
        self.connection_params = connection_params
        self.granularity_enum = granularity_enum
        self.srid = srid
    
        self.total_tpoints = total_tpoints
        self.create_matrix = create_matrix_fnc
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        pid = os.getpid()
        self.log(f"pid : {pid} QgisThread init |  CPU affinity : {psutil.Process(pid).cpu_affinity()} \n") 
            
    
        self.result_params = None
        self.error_msg = None


    def finished(self, result):
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)


    def run(self):
        """
        Runs the new process to create the matrix for the given time delta.
        """
        try:
        
            pid = os.getpid()
            self.log(f"pid : {pid} QgisThread run |  CPU affinity : {psutil.Process(pid).cpu_affinity()} \n") 
            
        
            result_queue = multiprocessing.Queue()
            
            # self.log(f"arguments : begin_frame : {self.begin_frame}, end_frame : {self.end_frame}, TIME_DELTA_SIZE : {TIME_DELTA_SIZE}, PERCENTAGE_OF_OBJECTS : {PERCENTAGE_OF_OBJECTS}, {self.extent}, len timestamps :{len(self.timestamps)}, granularity : {GRANULARITY.value},{len(self.objects_id_str)}")
            process = multiprocessing.Process(target=self.create_matrix, args=(result_queue, self.begin_frame, self.end_frame, self.time_delta_size, self.start_date, self.p_start, self.p_end, self.connection_params,  self.granularity_enum, self.srid, self.total_tpoints))
            process.start()                                                
            # self.log(f"Process started")
        
            return_value = result_queue.get()
            if return_value == 1:
                error = result_queue.get()
                self.log(f"Error inside new process: {error}")
                self.log(f"log for error: {result_queue.get()}")
                self.result_params = {
                    'matrix' : result_matrix
                }
                return True
            else:
                # Retrieve the result from the queue
                result_matrix = result_queue.get()
                logs= result_queue.get()
                result_queue.close()
                process.join()  # Wait for the process to complete
                self.log(logs)
                # self.log(f"Retrieved matrix shape: {result_matrix.shape}, logs {logs}" )

                self.result_params = {
                    'matrix' : result_matrix
                }
     
        except Exception as e:
            self.log(f"Error in run method : {e}")
            self.error_msg = str(e)
            return False
        return True
    

    def log(self, msg):
        """
        Function to log messages in the QGIS log window.
        """
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)







class MoveTask(QgsTask):
    def __init__(self, description, query, project_title, db, finished_fnc,
                 failed_fnc):
        super(MoveTask, self).__init__(description, QgsTask.CanCancel)
        self.query = query
        self.project_title = project_title
        self.db = db
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        self.result_params = None
        self.error_msg = None

    def finished(self, result):
        if result:
            self.finished_fnc(self.db, self.query, self.result_params)
        else:
            self.failed_fnc(self.error_msg)


class MoveGeomTask(MoveTask):
    def __init__(self, description, query, project_title, db, finished_fnc,
                 failed_fnc):
        super(MoveGeomTask, self).__init__(description, query, project_title,
                                           db, finished_fnc, failed_fnc)

    def run(self):
        try:
            view_name, col_names, srids, geom_types = self.query.create_geom_view(
                self.project_title, self.db)
            self.result_params = {
                'view_name': view_name,
                'col_names': col_names,
                'srids': srids,
                'geom_types': geom_types
            }
        except psycopg2.Error as e:
            self.error_msg = str(e)
            return False
        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True


class MoveTTask(MoveTask):
    def __init__(self, description, query, project_title, db, col_id,
                 finished_fnc, failed_fnc):
        super(MoveTTask, self).__init__(description, query, project_title, db,
                                        finished_fnc, failed_fnc)
        self.col_id = col_id

    def run(self):
        try:
            type, view_name, srid = self.query.create_temporal_view(
                self.project_title, self.db, self.col_id)
            
            self.result_params = {
                "type": type,
                'col_id': self.col_id,
                'view_name': view_name,
                'srid': srid
            }
            
        except psycopg2.Error as e:
            self.error_msg = e.diag.message_primary
            return False
        return True
    