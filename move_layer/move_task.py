import psycopg2

from qgis.core import QgsTask


from qgis.core import QgsMessageLog
from qgis.core import Qgis



class Next_Time_Delta_thread(QgsTask):
    """
    This thread creates next time delta's the matrix containing the positions for all objects to show. 
    """
    def __init__(self, description,project_title, start_date, granularity_enum, beg_frame, end_frame, extent, db, n_objects, srid, finished_fnc, failed_fnc):
        super(Next_Time_Delta_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title
        self.start_date = start_date
        self.granularity_enum = granularity_enum
        self.begin_frame = beg_frame
        self.end_frame = end_frame
        self.extent = extent
        self.db = db
        self.n_objects = n_objects
        self.srid = srid
       
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc


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
            start_time = self.start_date +  ( self.granularity_enum.value["timedelta"] * self.begin_frame) # TODO : 
            end_time = self.start_date +  ( self.granularity_enum.value["timedelta"] * self.end_frame) # TODO :
            rows = self.db.get_tgeompoints(start_time, end_time, self.extent, self.srid, self.n_objects)


            self.result_params = {
                'TgeomPoints_list' : rows
            }
        except Exception as e:
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
    
