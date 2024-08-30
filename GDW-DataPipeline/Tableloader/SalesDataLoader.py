from utils.utils import get_current_date


class SalesDataLoader:

    def __init__(self, spark,sc,input_filepath, run_date, gcp_project, stage_table, stage_ds, target_table, target_ds):
        self.input = input_filepath
        self.run_date = run_date
        self.stage_table = stage_table
        self.stage_ds = stage_ds
        self.target_table = target_table
        self.target_ds = target_ds
        self.project = gcp_project
        self.spark = spark
        self.sc = sc

    def run(self):
        print('==============')
        self.run_date = '2024-08-29'
        table_path = self.input + '/'  + self.target_table + '/' +  self.run_date + '/*.csv'
        print(table_path)
        raw_df = self.spark.read.format('csv').option('header',True).load(table_path)
        raw_df.show(10,False)
        stage_table = self.stage_ds + '.' + self.stage_table
        raw_df.write.format('bigquery').mode('overwrite').option('table',stage_table).save()
        print('wrirte completed------------------')




