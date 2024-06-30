

from .move_layer_controller import MoveLayerController
from .move_database_connector import DatabaseConnector
from .move_task import Matrix_generation_thread
from .move_spawned_process import create_matrix

from pymeos.db.psycopg import MobilityDB
from pymeos import *

from qgis.core import QgsMessageLog
from qgis.core import Qgis
from qgis.core import (

    QgsFeature,
    QgsDateTimeRange,
    QgsInterval,
    QgsGeometry,
)
from PyQt5.QtCore import QDateTime
import math
import time 
import psutil
import os 


class MoveLayerHandler:
    """
    Logic to handle the time deltas during the animation AND the data stored in memory.
    """

    def __init__(self, iface, connection_parameters, tm, time_delta_size, percentage_of_objects, SRID, granularity_enum, start_tdelta_frame=(0,0)):
        self.fps = 100
        self.time_delta_size = time_delta_size
        self.srid = SRID
        self.granularity_enum = granularity_enum
        self.connection_parameters = connection_parameters
        self.task_manager = tm
        self.iface = iface
        pymeos_initialize()
        
        self.move_layer_controller = MoveLayerController(self.iface, self.srid, self.fps)
        self.move_layer_controller.temporalController.updateTemporalRange.connect(self.on_new_frame)
        
        self.extent = self.move_layer_controller.get_canvas_extent()
        self.db = DatabaseConnector(self.connection_parameters, self.extent, percentage_of_objects, self.srid)
        self.generate_timestamps()


        self.initiate_temporal_controller_values()

        start_tdelta_key = start_tdelta_frame[0]
        start_frame = start_tdelta_frame[1]
        

        # variables to keep track of the current state of the animation
        self.current_time_delta_key = start_tdelta_key
        self.current_time_delta_end = start_tdelta_key + self.time_delta_size - 1
        self.previous_frame = start_frame
        self.direction = 1 # 1 : forward, 0 : backward
        self.changed_key = False # variable used to handle the scenario where a user moves forward and backward on a time delta boundary tick
        
        # Matrix variables
        self.previous_matrix = None
        self.current_matrix = None
        self.next_matrix = None

        self.objects_count = self.db.get_objects_count()
        self.objects_id_str = self.db.get_objects_str()
   
        

        # Create qgis features for all objects to display
        self.generate_qgis_features(self.objects_count, self.move_layer_controller.vlayer.fields(), self.timestamps[0], self.timestamps[-1])

        # Initiate request for first batch
        time_delta_key = start_tdelta_key
        beg_frame = time_delta_key
        end_frame = (time_delta_key + self.time_delta_size) -1
        self.last_recorded_time = time.time()

        task_matrix_gen = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.time_delta_size , self.connection_parameters, self.granularity_enum, self.srid,  self.create_matrix, self.initiate_animation, self.raise_error)
        self.task_manager.addTask(task_matrix_gen)     

    # Getters and Setters
    def get_current_time_delta_key(self):
        return self.current_time_delta_key
    
    def get_last_frame(self):
        return self.previous_frame


    # Methods to handle initial setup 

    def generate_qgis_features(self,num_objects, vlayer_fields,  start_date, end_date):
        features_list =[]
        start_datetime_obj = QDateTime(start_date)
        end_datetime_obj = QDateTime(end_date)


        for _ in range(num_objects):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([start_datetime_obj, end_datetime_obj])
            features_list.append(feat)
        
        self.move_layer_controller.set_qgis_features(features_list)
        self.log(f"{num_objects} Qgis features created")
        


    def initiate_animation(self, params):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        self.current_matrix = params['matrix']
        matrix_time = time.time() - self.last_recorded_time
        self.set_frame_rate(matrix_time)


        # Request for second time delta
        second_time_delta_key = self.time_delta_size
        self.fetch_next_data(second_time_delta_key)
        self.update_vlayer_features()

    
    def generate_timestamps(self):
        """
        Generate the timestamps associated to the dataset and the granularity selected.
        """
     
        start_date = self.db.get_min_timestamp()
        end_date = self.db.get_max_timestamp()
        self.total_frames = math.ceil( (end_date - start_date) // self.granularity_enum.value["timedelta"] ) + 1
        remainder_frames = (self.total_frames) % self.time_delta_size
        self.total_frames +=  remainder_frames

        self.timestamps = [start_date + i * self.granularity_enum.value["timedelta"] for i in range(self.total_frames)]
        self.timestamps = [dt.replace(tzinfo=None) for dt in self.timestamps]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]
  
  

    def initiate_temporal_controller_values(self):
        """
        Update the temporal controller values for the dataset
        """
        
        time_range = QgsDateTimeRange(self.timestamps[0], self.timestamps[-1])
        interval = QgsInterval(self.granularity_enum.value["steps"], self.granularity_enum.value["qgs_unit"])
        frame_rate = self.fps
        
        self.move_layer_controller.set_temporal_controller_extent(time_range) 
        self.move_layer_controller.set_temporal_controller_frame_duration(interval)
        self.move_layer_controller.set_temporal_controller_frame_rate(frame_rate)


    
    # Methods to handle the animation and t_delta logic

    def shift_matrices(self):
        self.previous_matrix = self.current_matrix
        self.current_matrix = self.next_matrix
        self.next_matrix = None 


    # def resume_animation(self):
    #     """
    #     PLays the animation in the current direction.
    #     """
    #     self.move_layer_controller.play(self.direction) #TODO
            

    def fetch_next_data(self, time_delta_key):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        pid = os.getpid()
        self.log(f"Qgis process pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}")
        
        
        beg_frame = time_delta_key
        end_frame = (time_delta_key + self.time_delta_size) -1
        self.log(f"Fetching data for time delta {beg_frame} : {end_frame}")
        if end_frame  <= self.total_frames and beg_frame >= 0: #Either bound has to be valid 
            self.last_recorded_time = time.time()

            task = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps,self.time_delta_size , self.connection_parameters, self.granularity_enum, self.srid, self.create_matrix, self.set_matrix, self.raise_error)
            self.task_manager.addTask(task)        


        
    def on_new_frame(self):
        """
        Handles the logic at each frame change.
        """

        frame_number = self.move_layer_controller.get_current_frame_number()
        if self.previous_frame - frame_number <= 0:
            self.direction = 1 # Forward
            if frame_number >= self.total_frames: # Reached the end of the animation, pause
                self.move_layer_controller.pause()
        else:
            self.direction = 0
            if frame_number <= 0: # Reached the beginning of the animation, pause
                self.move_layer_controller.pause()
            
        self.previous_frame = frame_number

        if frame_number % self.time_delta_size == 0:
            if self.direction == 1: # Animation is going forward
                # self.log(f"------- FETCH NEXT BATCH  - forward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                if self.current_time_delta_end + 1  != self.total_frames:
                    self.current_time_delta_key = frame_number
                    self.current_time_delta_end = (self.current_time_delta_key + self.time_delta_size) - 1
                    # self.log(f"------- FETCH NEXT BATCH  - forward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    self.shift_matrices()
                    self.fetch_next_data(self.current_time_delta_key+self.time_delta_size)                    
                    
                    # Pause the animation if the upcoming batch hasn't been fetched yet
                    if self.task_manager.countActiveTasks() != 0:
                        self.move_layer_controller.pause()
                    self.update_vlayer_features()
                    self.changed_key = True

                    
            else: # Animation is going backward
                # self.log(f"------- FETCH NEXT BATCH  - backward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                self.update_vlayer_features()  
                if self.current_time_delta_key != 0: 
                    self.current_time_delta_key = self.current_time_delta_key - self.time_delta_size
                    self.current_time_delta_end = frame_number-1
                    # self.log(f"------- FETCH NEXT BATCH  - backward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    
                    self.shift_matrices()

                    self.fetch_next_data(self.current_time_delta_key-self.time_delta_size)
                    # Pause the animation if the upcoming batch hasn't been fetched yet
                    if self.task_manager.countActiveTasks() != 0:
                        self.move_layer_controller.pause()

                    self.changed_key = True
                  
        else:
            if self.changed_key:
                if frame_number < self.current_time_delta_key:
                    self.current_time_delta_key = self.current_time_delta_key - self.time_delta_size
                    self.current_time_delta_end = frame_number
                    self.changed_key = False
            self.update_vlayer_features()
            self.changed_key = False
        

    def update_vlayer_features(self):
        """
        Updates the features of the vector layer for the given frame number.
        """
        try:
            time_delta_key = self.current_time_delta_key
            frame_number = self.previous_frame
            frame_index = frame_number- time_delta_key
     

            current_time_stamp_column = self.current_matrix[:, frame_index]
    
            new_geometries = {}  
            # new_geometries = {QgsGeometry().fromWkt(point) for point in current_time_stamp_column}  # Dictionary {feature_id: QgsGeometry}
            for i in range(current_time_stamp_column.shape[0]): #TODO : compare vs Nditer
                new_geometries[i] = QgsGeometry().fromWkt(current_time_stamp_column[i])


            self.move_layer_controller.vlayer.startEditing()
            # self.move_layer_controller.vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
            self.move_layer_controller.vlayer.dataProvider().changeGeometryValues(new_geometries) # Updating geometries for all features
            self.move_layer_controller.vlayer.commitChanges()
            self.iface.vectorLayerTools().stopEditing(self.move_layer_controller.vlayer)

        except Exception as e:
            self.log(f"Error updating the features for time_delta : {self.current_time_delta_key} and frame number : {self.previous_frame}")


    def clean_handler_memory(self):
        self.db.close()
        self.task_manager = None
        self.previous_matrix = None
        self.current_matrix = None
        self.next_matrix = None
        self.move_layer_controller.temporalController.updateTemporalRange.disconnect(self.on_new_frame)
        self.move_layer_controller.delete_vlayer()
        self.move_layer_controller = None


    # Methods to handle the QGIS threads


    def set_frame_rate(self, matrix_generation_time):
        uninterrupted_animation = self.time_delta_size / matrix_generation_time
        new_fps = min(uninterrupted_animation, self.fps)
        self.move_layer_controller.set_fps(new_fps)


    def set_matrix(self, params):
        """
        Assign the new matrix to its tdelta key.
        """
       
        # self.log("next matrix ready")
        self.next_matrix = params['matrix']
      

        TIME_Qgs_Thread = time.time() - self.last_recorded_time
        self.set_frame_rate(TIME_Qgs_Thread)
      
        


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")

    
    def log(self, msg):
        """
        Function to log messages in the QGIS log window.
        """
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)


