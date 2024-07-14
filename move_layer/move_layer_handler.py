

from .move_layer_controller import MoveLayerController
from .move_task import Next_Time_Delta_thread
# from .move_spawned_process import create_matrix, create_matrix_multi_cores

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
import pickle


class MoveLayerHandler:
    """
    Logic to handle the time deltas during the animation AND the data stored in memory.
    """

    def __init__(self, iface, db, tm, time_delta_size, n_objects, SRID, granularity_enum, fps, start_tdelta_key, start_frame):
        clear_log = "\n"*10
        self.log(clear_log)

        self.task_manager = tm
        # self.connection_parameters = connection_parameters
        self.db = db
        self.tfloat_columns = self.db.get_tfloat_columns()
        self.fps = fps
        self.iface = iface
        self.time_delta_size = time_delta_size
        self.n_objects = n_objects
        self.SRID = SRID
        self.granularity_enum = granularity_enum
        self.start_tdelta_key = start_tdelta_key
        self.start_frame = start_frame
        pymeos_initialize()
        
        self.move_layer_controller = MoveLayerController(self.iface, self.SRID, self.fps, self.tfloat_columns)
        
        self.extent = self.move_layer_controller.get_initial_canvas_extent()
        self.start_date = self.db.get_min_timestamp()
        self.end_date = self.db.get_max_timestamp()

        self.initiate_temporal_controller_values()

  
        # variables to keep track of the current state of the animation
        self.current_time_delta_key = start_tdelta_key
        self.current_time_delta_end = start_tdelta_key + self.time_delta_size - 1
        self.previous_frame = start_frame
        self.direction = 1 # 1 : forward, 0 : backward
        self.changed_key = False # variable used to handle the scenario where a user moves forward and backward on a time delta boundary tick
        
        self.objects_count = self.db.get_objects_count()
        
        self.previous_tpoints = None
        self.current_tpoints = None
        self.next_tpoints = None

        # Create qgis features for all objects
        self.generate_qgis_features()

        # Initiate request for first batch
        time_delta_key = start_tdelta_key
        beg_frame = time_delta_key
        end_frame = (time_delta_key + self.time_delta_size) -1
        self.last_recorded_time = time.time()
        self.qgis_task_records = []
        self.onf_records = []
        self.total_frames = 1440

        task = Next_Time_Delta_thread(f"Data for time delta {time_delta_key}","Solution C", self.start_date, self.granularity_enum, beg_frame, end_frame,
                                     self.extent, self.db, self.n_objects, self.SRID, self.initiate_animation, self.raise_error)
        self.task_manager.addTask(task)     
        self.move_layer_controller.temporalController.updateTemporalRange.connect(self.on_new_frame)
        # self.task_manager.allTasksFinished.connect(self.resume_animation)
        
    

    def update_geometries(self):
        """
        Updates the geometries of the features in the vector layer.
        # TODO : refresh current time delta here since load time is very short.
        """
        frame_number = self.previous_frame
        timestamp = self.start_date + self.granularity_enum.value["timedelta"]  * frame_number
        
        
        for i in range(1, self.n_objects+1):
            try:
                position = self.current_tpoints[i-1][0].value_at_timestamp(timestamp)
                self.geometries[i].fromWkb(position.wkb) 
            except:
                continue 
        
        for i in range(self.n_objects+1, self.objects_count+1):
            self.geometries[i] = QgsGeometry()

        self.move_layer_controller.vlayer.startEditing()
        self.move_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries) # Updating geometries for all features
        self.move_layer_controller.vlayer.commitChanges()
        self.iface.vectorLayerTools().stopEditing(self.move_layer_controller.vlayer)


        self.geometries = {}
        for i in range(1, self.n_objects+1):
            geom = QgsGeometry()
            self.geometries[i] = geom
            self.move_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)


    def  pause_animation(self):
        self.move_layer_controller.pause_animation()

    def set_nobjects(self, n_objects):
        if n_objects != self.n_objects:
            self.n_objects = n_objects
            self.update_geometries()
        
        

    def get_current_time_delta_key(self):
        return self.current_time_delta_key
    
    def get_last_frame(self):
        return self.previous_frame

    # Methods to handle initial setup 

    def generate_qgis_features(self):
        features_list =[]
        object_ids = self.db.get_objects_ids()

        start_datetime_obj = QDateTime(self.start_date)
        end_datetime_obj = QDateTime(self.end_date)
        num_objects = self.objects_count
        vlayer_fields = self.move_layer_controller.get_vlayer_fields()
        
        self.geometries={}
        for i in range(1, num_objects+1):
            feat = QgsFeature(vlayer_fields)
            attributes_list = [ object_ids[i-1][0],start_datetime_obj, end_datetime_obj]
            for tfloat in self.tfloat_columns:
                attributes_list.append( 0.0 )
            feat.setAttributes(attributes_list)
            geom = QgsGeometry()
            self.geometries[i] = geom
            feat.setGeometry(geom)
            features_list.append(feat)
        
        self.move_layer_controller.add_features(features_list)
        
    
    def update_fps(self, new_fps):
        self.fps = new_fps
        self.move_layer_controller.update_fps_cap(new_fps)
        


    def initiate_animation(self, params):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        self.current_tpoints = params['TgeomPoints_list']

        qgis_task_time = time.time() - self.last_recorded_time
        self.log(f"Time delta generation time : {qgis_task_time}")
        self.qgis_task_records.append(qgis_task_time)
 
        # Request for second time delta
    
        second_time_delta_key = self.time_delta_size
        self.fetch_next_data(second_time_delta_key)
        self.update_vlayer_features()
        # self.resume_animation()
  



    def initiate_temporal_controller_values(self):
        """
        Update the temporal controller values for the dataset
        """
        
        time_range = QgsDateTimeRange(self.start_date, self.end_date)
        interval = QgsInterval(self.granularity_enum.value["steps"], self.granularity_enum.value["qgs_unit"])
        frame_rate = self.fps
        
        self.move_layer_controller.set_temporal_controller_extent(time_range) 
        self.move_layer_controller.set_temporal_controller_frame_duration(interval)
        self.move_layer_controller.set_temporal_controller_frame_rate(frame_rate)


    
    # Methods to handle the animation and t_delta logic

    def shift_matrices(self):
        self.previous_tpoints = self.current_tpoints
        self.current_tpoints = self.next_tpoints
        self.next_tpoints = None



    def resume_animation(self):
        """
        PLays the animation in the current direction.
        """
        self.move_layer_controller.play(self.direction) #TODO
            

    def fetch_next_data(self, time_delta_key):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
    
        beg_frame = time_delta_key
        end_frame = (time_delta_key + self.time_delta_size) -1
        self.log(f"Fetching data for time delta {beg_frame} : {end_frame}")
        if end_frame  <= self.total_frames and beg_frame >= 0: #Either bound has to be valid 
            self.last_recorded_time = time.time()
            # self.move_layer_controller.pause_animation()
            current_extent = self.move_layer_controller.get_current_canvas_extent()

            if current_extent != self.extent:
                self.extent = current_extent
                self.move_layer_controller.pause_animation()
                self.iface.messageBar().pushMessage("Info", "Animation has paused to adapt to new canvas", level=Qgis.Info)
            
            task = Next_Time_Delta_thread(f"Data for time delta {time_delta_key}","Solution C", self.start_date, self.granularity_enum, beg_frame, end_frame,
                                     self.extent, self.db, self.n_objects, self.SRID, self.on_thread_complete, self.raise_error)
            self.task_manager.addTask(task)        


        
    def on_new_frame(self):
        """
        Handles the logic at each frame change.
      
        """
        onf_time = time.time() 
        frame_number = self.move_layer_controller.temporalController.currentFrameNumber()
        forward = self.previous_frame + 1
        backward = self.previous_frame - 1
        if frame_number == forward:
            # print("YOOHOO FORWARD")
            self.direction = 1 # Forward
            # if frame_number == 120: # Reached the end of the animation, pause
            #     self.log("dsfjsdf")
            #     self.move_layer_controller.pause_animation()

            #     with open(f"/home/ali/move_layer/move_layer/laptop_results/Solution_C_{self.time_delta_size}_{self.n_objects}_onf_record.pickle", "wb") as file:
            #         pickle.dump(self.onf_records, file)

            #     with open(f"/home/ali/move_layer/move_layer/laptop_results/Solution_C_{self.time_delta_size}_{self.n_objects}_qgis_record.pickle", "wb") as file:
            #         pickle.dump(self.qgis_task_records, file)

            #     with open(f"/home/ali/move_layer/move_layer/laptop_results/Solution_C_{self.time_delta_size}_{self.n_objects}_fps_record.pickle", "wb") as file:
            #         pickle.dump(self.move_layer_controller.fps_record, file)

        elif frame_number == backward:
            # print("YOOHOO BACKWARD")
            self.direction = 0
            if frame_number <= 0: # Reached the beginning of the animation, pause
                self.move_layer_controller.pause_animation()
        else:
            print("HANDLE TEMPORAL CONTROLLER STATE CHANGE")
            print(f"frame_number : {frame_number}, previous_frame : {self.previous_frame}") 
            return False

        self.previous_frame = frame_number

        if frame_number % self.time_delta_size == 0:
            if self.direction == 1: # Animation is going forward
                # log(f"------- FETCH NEXT BATCH  - forward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                if self.current_time_delta_end + 1  != self.total_frames:
                    self.current_time_delta_key = frame_number
                    self.current_time_delta_end = (self.current_time_delta_key + self.time_delta_size) - 1
                    # log(f"------- FETCH NEXT BATCH  - forward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    self.shift_matrices()

                    if self.task_manager.countActiveTasks() != 0:
                        self.move_layer_controller.pause_animation()

                    self.fetch_next_data(self.current_time_delta_key+self.time_delta_size)                    
                    self.update_vlayer_features()
                    self.changed_key = True

                    
            else: # Animation is going backward
                # log(f"------- FETCH NEXT BATCH  - backward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                self.update_vlayer_features()  
                if self.current_time_delta_key != 0: 
                    self.current_time_delta_key = self.current_time_delta_key - self.time_delta_size
                    self.current_time_delta_end = frame_number-1
                    # log(f"------- FETCH NEXT BATCH  - backward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    
                    self.shift_matrices()
                    if self.task_manager.countActiveTasks() != 0:
                        self.move_layer_controller.pause_animation()

                    self.fetch_next_data(self.current_time_delta_key-self.time_delta_size)
                    self.changed_key = True
            # Calculating the optimal FPS based on the new frame time
            optimal_fps = 1/ (time.time()- onf_time)
            self.onf_records.append(optimal_fps)
            self.move_layer_controller.update_frame_rate(optimal_fps)
                  
        else:
            if self.changed_key:
                if frame_number < self.current_time_delta_key:
                    self.current_time_delta_key = self.current_time_delta_key - self.time_delta_size
                    self.current_time_delta_end = frame_number
                    self.changed_key = False
            self.update_vlayer_features()
            self.changed_key = False
            # Calculating the optimal FPS based on the new frame time
            optimal_fps = 1/ (time.time()- onf_time)
            self.onf_records.append(optimal_fps)
            self.move_layer_controller.update_frame_rate(optimal_fps)
        

    def update_vlayer_features(self):
        """
        Updates the features of the vector layer for the given frame number.
        """
        try:
            # time_delta_key = self.current_time_delta_key
            frame_number = self.previous_frame
            timestamp = self.start_date + self.granularity_enum.value["timedelta"]  * frame_number
            
            tfloat_values = {}
            for i in range(1, self.n_objects+1):
                try:
                    position = self.current_tpoints[i-1][0].value_at_timestamp(timestamp)
                    self.geometries[i].fromWkb(position.wkb) 

                    if len(self.tfloat_columns) > 0:
                        attributes = {}
                        for j in range(len(self.tfloat_columns)):
                            attributes[j+3] = self.current_tpoints[i-1][j+1].value_at_timestamp(timestamp)
                        tfloat_values[i] = attributes
                except:
                    continue 

            self.move_layer_controller.vlayer.startEditing()
            self.move_layer_controller.vlayer.dataProvider().changeAttributeValues(tfloat_values) 
            self.move_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries) # Updating geometries for all features
            self.move_layer_controller.vlayer.commitChanges()
            self.iface.vectorLayerTools().stopEditing(self.move_layer_controller.vlayer)

        except Exception as e:
            self.log(f"Error updating the features {e} for time_delta : {self.current_time_delta_key} and frame number : {self.previous_frame}")


    # Methods to handle the QGIS threads
    def delete(self):
        self.db.close()
        pymeos_finalize()
        self.task_manager = None
        self.previous_matrix = None
        self.current_matrix = None
        self.next_matrix = None
        self.move_layer_controller.temporalController.updateTemporalRange.disconnect(self.on_new_frame)
        
    


    def set_frame_rate(self, new_fps):
        self.move_layer_controller.set_fps(new_fps)


    def on_thread_complete(self, params):
        """
        Assign the next time delta
        """
        self.log("Thread completed")
        self.next_tpoints = params['TgeomPoints_list']
      
        TIME_Qgs_Thread = time.time() - self.last_recorded_time
        self.log(f"time delta generation time : {TIME_Qgs_Thread}")
        self.qgis_task_records.append(TIME_Qgs_Thread)
      

    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")

    def log(self, msg):
        QgsMessageLog.logMessage(msg, "Move_layer", level=Qgis.Info)

