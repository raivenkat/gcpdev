from config.Config import Config
from utils.utils import get_current_date
from Tableloader.SalesDataLoader import SalesDataLoader
from logs.logger import Log
class GdwDataLoaderApplication:

    def __init__(self,input_filepath,target_table):
        self.spark,self.sc = Config().initializeSpark()
        self.run_date = get_current_date()
        self.input_file = input_filepath
        self.table_name = target_table
        self.stage_table = 'STG_' + target_table
        self.project = 'dev-de-training'
        self.stg_bq_dataset = 'default_dataset'
        self.target_bq_dataset = 'default_dataset'
        self.target_table = target_table
        self.logger = Log.logger


    def run(self):

        if self.table_name == 'SALES_DATA':
            try:

                print('*********** entered table block',self.table_name)
                sales_loader_obj = SalesDataLoader(self.spark,self.sc, self.input_file, self.run_date,
                                                   self.project, self.stage_table,
                                                   self.stg_bq_dataset,
                                                   self.target_table, self.target_bq_dataset)

                sales_loader_obj.run()
                # update control table
                self.logger.info(f"Application successfully completed and loaded data to big query---{self.target_table}")
            except Exception as e:
                self.logger.error(f"Application failed at {self.target_table} table load error msg is {e}")
                #update control table



