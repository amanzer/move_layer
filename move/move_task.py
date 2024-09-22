import psycopg

from qgis.core import QgsTask



class FetchDataThread(QgsTask):
    """
    A QGIS task to fetch TgeomPoints data from the database asynchronously.

    This class runs as a background task in QGIS, retrieving data from a database using
    the provided database connector, without blocking the User Interface. 
    """

    def __init__(self, description: str, project_title: str, database_connector, finished_fnc, failed_fnc):
        """
        Initialize the FetchDataThread.

        :param description: A description of the task.
        :param project_title: The title of the project for which data is being fetched.
        :param database_connector: An instance of the database connector used to fetch data.
        :param finished_fnc: A callback function to execute if the task finishes successfully.
        :param failed_fnc: A callback function to execute if the task fails.
        """
        super(FetchDataThread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        self.result_params = None
        self.error_msg = None
    
    def finished(self, result: bool) -> None:
        """
        Called when the task is finished.

        :param result: A boolean indicating if the task was successful.
        """
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)

    def run(self) -> bool:
        """
        Runs the task to fetch TgeomPoints from the database.

        :return: True if the task is successful, False if an error occurred.
        """
        try:
            # Fetch TgeomPoints from the database
            self.result_params = {
                'TgeomPoints_list': self.database_connector.get_TgeomPoints()
            }
        except Exception as e:
            # Capture and store any error message
            self.error_msg = str(e)
            return False
        return True


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
        except psycopg.Error as e:
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
            view_name, srid = self.query.create_temporal_view(
                self.project_title, self.db, self.col_id)
            self.result_params = {
                'col_id': self.col_id,
                'view_name': view_name,
                'srid': srid
            }
        except psycopg.Error as e:
            self.error_msg = e.diag.message_primary
            return False
        return True