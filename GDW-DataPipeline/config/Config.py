from pyspark import SparkConf
from pyspark.sql import SparkSession


class Config:

    def __init__(self):
        self.config = {
            'spark.driver.cores': '2',
            'spark.driver.memory': '5g',
            #'spark.executor.instances': '10',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.minExecutors': '1',
            'spark.dynamicAllocation.maxExecutors': '2',
            'spark.executor.memory': '5g',
            'spark.master': 'yarn',
            'spark.app.name': 'job_gdw_data_loader',
            'spark.sql.shuffle.partitions': '100',
            'temporaryGcsBucket': 'dev-de-training-default',
            'viewsEnabled': 'true',
            'materializationDataset': 'default_dataset',
            'materializationExpirationTimeInMinutes': '30'
        }

    def initializeSpark(self):
        conf = SparkConf()
        for option in list(self.config.keys()):
            conf.set(option,self.config[option].replace(" ",""))
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        sc = spark.sparkContext
        return spark,sc

