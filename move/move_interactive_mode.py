from pymeos.db.psycopg import MobilityDB
from pymeos import *
import time
import uuid
from shapely.geometry import Point

from .move_task import FetchDataThread

from qgis.PyQt.QtCore import QVariant

from qgis.core import (
    Qgis,
    QgsVectorLayerTemporalProperties,
    QgsFeature,
    QgsGeometry,
    QgsVectorLayer, 
    QgsField, 
    QgsProject,
    QgsMessageLog,    
)
from PyQt5.QtCore import QDateTime




def log(msg):
    QgsMessageLog.logMessage(msg, 'Move', level=Qgis.Info)



class DatabaseController:
    """
    Singleton class to handle the MobilityDB connection.
    """
    def __init__(self, connection_parameters: dict, limit: input):
        """
        Initialize the DatabaseController with connection parameters.

        :param connection_parameters: A dictionary containing database connection parameters.
        """
        try:
            self.limit = limit
            self.connection_params = {
                "host": connection_parameters["host"],
                "port": connection_parameters["port"],
                "dbname": connection_parameters["dbname"],
                "user": connection_parameters["user"],
                "password": connection_parameters["password"],
            }

            self.table_name = connection_parameters["table_name"]
            self.tpoint_column_name = connection_parameters["tpoint_column_name"]

        except KeyError as e:
            log(f"Missing connection parameter: {e}")
            raise

    def get_TgeomPoints(self) -> list:
        """
        Fetch TgeomPoints from the database.

        :return: A list of TgeomPoints fetched from the database.
        """
        try:
            query = (
                f"SELECT {self.tpoint_column_name}, "
                f"startTimestamp({self.tpoint_column_name}), "
                f"endTimestamp({self.tpoint_column_name}) "
                f"FROM public.{self.table_name} LIMIT {self.limit};"
            )
            log(f"Executing query: {query}")

            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)

            results = []
            while True:
                rows = self.cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)


            query_srid = f"SELECT SRID({self.tpoint_column_name}) FROM {self.table_name} LIMIT 1;"
            log(f"Executing query: {query_srid}")

            self.cursor.execute(query_srid)
            srid = self.cursor.fetchone()[0]


            self.cursor.close()
            self.connection.close()

            return results, srid

        except Exception as e:
            log(f"Error in fetching TgeomPoints: {e}")
            return None



class MobilitydbLayerHandler:
    """
    Initializes and handles the vector layer controller to display MobilityDB data.

    This class manages the creation and updating of an in-memory vector layer in QGIS
    that displays TgeomPoints data from a MobilityDB database. The data is fetched asynchronously
    at the start of the script and updated in real-time as the temporal controller advances.
    """

    def __init__(self, layers_list, iface, task_manager, limit: int, connection_parameters: dict):
        """
        Initialize the MobilitydbLayerHandler.

        :param iface: The QGIS interface instance.
        :param task_manager: The QGIS task manager instance for managing background tasks.
        :param srid: The spatial reference ID (SRID) for the vector layer's coordinate system.
        :param connection_parameters: A dictionary containing database connection parameters.
        """
        self.mobilitydb_layers_list = layers_list
        self.iface = iface
        self.task_manager = task_manager
        
        self.database_controller = DatabaseController(connection_parameters, limit)

        self.last_time_record = time.time()

        # Start a background task to fetch data from the MobilityDB database
        self.fetch_data_task = FetchDataThread(
            description="Fetching MobilityDB Data",
            project_title="Move",
            database_connector=self.database_controller,
            finished_fnc=self.on_fetch_data_finished,
            failed_fnc=self.raise_error
        )
        self.task_manager.addTask(self.fetch_data_task)

    def raise_error(self, msg: str) -> None:
        """
        Called when the task to fetch data from the MobilityDB database fails.

        :param msg: The error message.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")

    def on_fetch_data_finished(self, result_params: dict) -> None:
        """
        Callback function that is called when the data fetch task is completed.
        This creates the QGIS Features associated to each trajecotry and adds them to the vector layer.

        :param result_params: A dictionary containing the trajectories.
        """
        try:
            self.TIME_fetch_tgeompoints = time.time() - self.last_time_record
            results = result_params.get('TgeomPoints_list', [])
            log(f"Number of results: {len(results[0])}")

            srid = results[1]
            log(f"srid:  {srid}")
            self.vector_layer_controller = TemporaryLayerController(srid)
            vlayer_fields = self.vector_layer_controller.get_vlayer_fields()

            features_list = []
            self.geometries = {}
            self.tpoints = {}
            index = 1

            for row in results[0]:
                try:
                    self.tpoints[index] = row[0]
                    feature = QgsFeature(vlayer_fields)
                    feature.setAttributes([index, QDateTime(row[1]), QDateTime(row[2])])

                    geom = QgsGeometry()
                    self.geometries[index] = geom
                    feature.setGeometry(geom)
                    features_list.append(feature)
                    
                    index += 1

                except Exception as e:
                    log(f"Error creating feature: {e} \nRow: {row} \nIndex: {index}")

            # Add all features to the vector layer
            self.vector_layer_controller.add_features(features_list)
            self.objects_count = index - 1

            log(f"Time taken to fetch TgeomPoints: {self.TIME_fetch_tgeompoints}")
            log(f"Number of TgeomPoints fetched: {self.objects_count}")       

            results = None  # Free memory
            self.iface.messageBar().pushMessage("Info", "TGeomPoints have been loaded", level=Qgis.Info)

        except Exception as e:
            log(f"Error in on_fetch_data_finished: {e}")

    def new_frame(self, timestamp: QDateTime) -> None:
        """
        Update the layer with new geometries corresponding to the given timestamp.

        :param timestamp: The timestamp for which to update the layer's geometries.
        """
        try:
            log(f"New Frame: {timestamp}")
            visible_geometries = 0
            empty_geom = Point().wkb

            for i in range(1, self.objects_count + 1):
                try:
                    # Fetch the position of the object at the current timestamp
                    position = self.tpoints[i].value_at_timestamp(timestamp)

                    # Update the geometry of the feature in the vector layer
                    self.geometries[i].fromWkb(position.wkb)
                    visible_geometries += 1
                except:
                    # Set geometry to empty if position is not available
                    self.geometries[i].fromWkb(empty_geom)

            log(f"{visible_geometries} visible geometries")

            # Update the geometries in the vector layer
            self.vector_layer_controller.vlayer.startEditing()
            self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
            self.vector_layer_controller.vlayer.commitChanges()
        except Exception as e:
            try:
                if self.vector_layer_controller.vlayer:
                    log(f"Error in new_frame: {e}")
            except: # If the vector layer has been deleted remove it from the spatiotemporal layer's list
                self.vector_layer_controller = None
                self.mobilitydb_layers_list.remove(self)

                


class TemporaryLayerController:
    """
    Controller for an in-memory vector layer to view TgeomPoints.

    This class creates and manages a in-memory vector layer within QGIS, specifically designed
    to handle Spatial Points with temporal properties.
    """

    def __init__(self, srid: int):
        """
        Initialize the VectorLayerController with a given SRID.

        :param srid: Spatial Reference Identifier (SRID) used to define the coordinate system.
        """
        # Create an in-memory vector layer with the specified SRID
        self.vlayer = QgsVectorLayer(f"Point?crs=epsg:{srid}", f"MobilityDB layer - {uuid.uuid4().hex}", "memory")

        # Define the fields for the vector layer
        fields = [
            QgsField("id", QVariant.Int),
            QgsField("start_time", QVariant.DateTime),
            QgsField("end_time", QVariant.DateTime)
        ]
        self.vlayer.dataProvider().addAttributes(fields)
        self.vlayer.updateFields()

        # Define the temporal properties of the vector layer
        temporal_properties = self.vlayer.temporalProperties()
        temporal_properties.setIsActive(True)
        temporal_properties.setMode(QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        temporal_properties.setStartField("start_time")
        temporal_properties.setEndField("end_time")

        self.vlayer.updateFields()

        # Add the vector layer to the QGIS project
        QgsProject.instance().addMapLayer(self.vlayer)

    def get_vlayer_fields(self):
        """
        Get the fields of the vector layer.

        :return: A list of fields if the vector layer exists, otherwise None.
        """
        try:
            return self.vlayer.fields()
        except Exception as e:
            log(f"Error in get_vlayer_fields: {e}")


    def add_features(self, features_list: list):
        """
        Add features to the vector layer.

        :param features_list: A list of QgsFeature objects to be added to the vector layer.
        """
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)
            self.vlayer.updateExtents()  # Update the layer's extent after adding features

