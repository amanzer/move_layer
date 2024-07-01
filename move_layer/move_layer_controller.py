import time
from qgis.core import (
    QgsVectorLayer, QgsField, QgsProject,
    QgsCoordinateReferenceSystem
)
from qgis.PyQt.QtCore import QVariant

from qgis.core import QgsVectorLayerTemporalProperties
from qgis.core import QgsMessageLog
from qgis.core import Qgis


class MoveLayerController:
    """

    This class plays the role of the controller in the MVC pattern.
    It is used to manage the user interaction with the View, which is the QGIS UI.
    
    It handles the interactions with both the Temporal Controller and the Vector Layer.
    """
    def __init__(self, iface, srid, FPS=60):
        self.srid = srid
        self.iface = iface    
        self.canvas = self.iface.mapCanvas()
        self.create_vlayer()
        
        self.temporalController = self.canvas.temporalController()
        self.temporalController.setCurrentFrameNumber(0)
        self.extent = self.canvas.extent().toRectF().getCoords()

        self.fps = FPS

        self.fps_record = []
        
 

    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
        """
        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem(f"EPSG:{self.srid}")) # TODO : not needed

        self.vlayer = QgsVectorLayer("Point", "MobilityBD Data", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        tp.setMode(QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
       
        tp.setStartField("start_time")
        tp.setEndField("end_time")
        self.vlayer.updateFields()
        # crs = self.vlayer.crs()
        # crs.createFromId(self.srid)
        # self.vlayer.setCrs(crs)
        QgsProject.instance().addMapLayer(self.vlayer) # TODO : replaced method


    # Getters

    def get_canvas_extent(self):
        return self.extent
    

    def get_current_frame_number(self):

        return self.temporalController.currentFrameNumber()

    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """

        return sum(self.fps_record)/len(self.fps_record)


    # Setters 

    #TODO : Need to define getters for when Temporal Controller state is changed by the user
    def set_temporal_controller_extent(self, time_range):
        if self.temporalController:
            self.temporalController.setTemporalExtents(time_range)
    

    def set_temporal_controller_frame_duration(self, interval):
        if self.temporalController:
            self.temporalController.setFrameDuration(interval)
    

    def set_temporal_controller_frame_rate(self, frame_rate):
        if self.temporalController:
            self.temporalController.setFramesPerSecond(frame_rate)


    def set_temporal_controller_frame_number(self, frame_number):
        if self.temporalController:
            self.temporalController.setCurrentFrameNumber(frame_number)

    def set_qgis_features(self, features_list):
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)

    def set_fps(self, fps):
        self.fps = fps

    # Methods to handle the temporal controller
    
    def play(self, direction):
        """
        Plays the temporal controller animation in the given direction.
        """
        if direction == 1:
            self.temporalController.playForward()
        else:
            self.temporalController.playBackward()


    def pause(self):
        """
        Pauses the temporal controller animation.
        """
        self.temporalController.pause()


    def update_frame_rate(self, new_frame_time):
        """
        Updates the frame rate of the temporal controller to be the closest multiple of 5,
        favoring the lower value in case of an exact halfway.
        """
        # Calculating the optimal FPS based on the new frame time
        optimal_fps = 1 / new_frame_time
        # Ensure FPS does not exceed 60
        fps = min(optimal_fps, self.fps)

        self.temporalController.setFramesPerSecond(fps)
        self.log(f"FPS : {fps}      |      ONF : {optimal_fps}      |      Matrix {self.fps}")
        self.fps_record.append(optimal_fps)


    
    def delete_vlayer(self):
        QgsProject.instance().removeMapLayer(self.vlayer.id())
   
    def log(self, msg):
        """
        Function to log messages in the QGIS log window.
        """
        QgsMessageLog.logMessage(msg, f'Framerate', level=Qgis.Info)

    # def delete_vlayer(self):
    #     start_tdelta_key = self.handler.get_current_time_delta_key()
    #     start_frame = self.handler.get_last_frame()

    #     self.handler.delete()
        
    #     self.create_vlayer()
    #     self.set_temporal_controller_frame_number(start_frame)
    #     self.extent = self.canvas.extent().toRectF().getCoords()

    #     self.handler = Time_deltas_handler(self, (start_tdelta_key ,start_frame))
    #     self.fps_record = []
