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

        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem(f"EPSG:{self.srid}"))
        self.temporalController = self.canvas.temporalController()
        self.temporalController.setCurrentFrameNumber(0)
        self.extent = self.canvas.extent().toRectF().getCoords()

        self.fps_cap = FPS
        
        self.fps_record = []
        self.onf_record = []


 

    def add_features(self, features_list):
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)
            

    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
        """
        self.vlayer = QgsVectorLayer("Point", "MobilityBD Data", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("id", QVariant.Int) ,QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        # tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)
        tp.setMode(QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        # tp.setStartField("time")
        tp.setStartField("start_time")
        tp.setEndField("end_time")
        self.vlayer.updateFields()

        QgsProject.instance().addMapLayer(self.vlayer)


    # Getters

    def get_current_canvas_extent(self):
        return self.canvas.extent().toRectF().getCoords()

    def get_initial_canvas_extent(self):
        return self.extent
    
    def get_vlayer_fields(self):
        if self.vlayer:
            return self.vlayer.fields()
        return None

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

    def set_fps_cap(self, fps):
        self.fps_cap = fps

    # Methods to handle the temporal controller
    
    def play(self, direction):
        """
        Plays the temporal controller animation in the given direction.
        """
        if direction == 1:
            self.temporalController.playForward()
        else:
            self.temporalController.playBackward()

    def update_fps_cap(self, new_fps_cap):
        """
        Updates the frame rate cap of the temporal controller.
        """
        self.fps_cap = new_fps_cap

    def pause_animation(self):
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
        fps = min(optimal_fps, self.fps_cap)

        self.temporalController.setFramesPerSecond(fps)
        self.log(f"FPS : {fps}      |      ONF : {optimal_fps}      |   fps cap {self.fps_cap}")
        self.fps_record.append(optimal_fps)
    
    def log (self, message):
        QgsMessageLog.logMessage(message, level=Qgis.Info)
    


