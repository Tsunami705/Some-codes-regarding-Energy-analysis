#!/usr/bin/env python3

import gc, os

from conf import ApiConf
from datetime import datetime
from hdfs.client import Client


from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql import Window as win

class Analyze:
    ''' Business Analysis '''
    IN_PATH = f'{ApiConf.WORK_DIR}/dat/{ApiConf.IN_FILE}'
    OUT_PATH = f'{ApiConf.WORK_DIR}/dat/{ApiConf.OUT_FILE}'
    
    def __init__(self, params):
         '''Select Business Execution Analysis '''
         self.job = params['job']
         self.hdfs_in = f'/data/{self.job}/input/input.json'
         self.hdfs_out = f'/data/{self.job}/output'
         self.client = Client(ApiConf.HDFS_URL,'/')
        
         self. upload()
        
         if os.path.exists(Analyze.IN_PATH):
             os. remove(Analyze. IN_PATH)
            
         if self.job == 'pur_dly':
             self. pur_dly()
         else:
             raise ValueError(f'Cannot find parameter: {self.job}')
        
    
    def upload(self):
         ''' Upload analysis input file to HDFS '''
         try:
             self.client.upload(self.hdfs_in, Analyze.IN_PATH)
         except Exception as err:
             ApiConf().get_file_logger('spark').error(err)
             self.client.delete(self.hdfs_in, recursive=True)
             del self.client
             raise Exception(err)
        
    def start(self):
         ''' Write analysis results to file
        
             Read JSON file from HDFS and write to local, delete HDFS directory
            
             return:
                 0 = success, 1 = failure
         '''
         if os.path.exists(Analyze.OUT_PATH):
             os. remove(Analyze. OUT_PATH)
        
         try:
             for file in self.client.list(self.hdfs_out):
                 if file.endswith('.json'):
                     with self.client.read(f"{self.hdfs_out}/{file}") as hf:
                         with open(Analyze.OUT_PATH,'ab') as lf:
                             lf.write(hf.read())
         except Exception as err:
             ApiConf().get_file_logger('spark').error(err)
             self.client.delete(self.hdfs_in, recursive=True)
             self.client.delete(self.hdfs_out, recursive=True)
             del self.client
             raise Exception(err)
        
         self.client.delete(self.hdfs_in, recursive=True)
         self.client.delete(self.hdfs_out, recursive=True)
        
         del self.client
         gc. collect()
         return 0
    
    def pur_dly(self):
        ''' Calculate and count the completion rate, on-time rate, and average delay days by material ID '''
        try:
            spark = SparkSession. builder \
                 .master(ApiConf.SP_URL) \
                 .appName("purchase_delay")\
                 .getOrCreate()
            
            spark.sparkContext.setLogLevel('Error')
            
            rate = spark.read.json(self.hdfs_in)

            rate = rate.filter((rate['采购数量'] >= 1)|(rate['采购数量'] == 0))
            rate = rate.select(
                '采购订单分录ID','采购入库头ID','采购入库分录ID',
                '采购组织','供应商','采购员','采购订单编号','工程号',
                '助记码','物料名称','规格型号','存货类别',
                '采购数量','入库实收数量','入库退料数量','未入库数量',
                '采购日期','计划到货日期','入库创建日期',
                
                fun.concat_ws(
                    '_','采购订单头ID','采购订单分录ID'
                ).alias('ORD_ID'),
                (rate['入库实收数量'] - rate['入库退料数量']).alias('IN_NUM'),
            )
            
            win_ord = win.orderBy('ORD_ID').partitionBy('ORD_ID')
            
            rate = rate.withColumn(
                'IN_SUM',
                fun.sum('IN_NUM').over(win_ord)
            ).withColumn(
                'NEXT_IN',
                fun.lag('入库创建日期').over(win_ord)
            )
            
            rate = rate.withColumn(
                'OUT_NUM',
                rate['采购数量'] - rate['IN_SUM']
            ).withColumn(
                'NEXT_IN',
                fun.when(
                    rate['NEXT_IN'].isNull(),
                    fun.current_date()
                ).otherwise(rate['NEXT_IN'])
            )
            
            rate = rate.withColumn(
                'IN_DLY_DAYS',
                fun.datediff(rate['入库创建日期'],rate['计划到货日期'])
            ).withColumn(
                'OUT_DLY_DAYS',
                fun.when(
                    rate['OUT_NUM'] == 0, 0
                ).otherwise(
                    fun.datediff(rate['NEXT_IN'],rate['计划到货日期'])
                )
            )
            
            rate = rate.withColumn(
                'IN_IS_DLY',
                fun.when(rate['IN_DLY_DAYS'] <= 0, 0).otherwise(1)
            ).withColumn(
                'OUT_IS_DLY',
                fun.when(rate['OUT_DLY_DAYS'] <= 0, 0).otherwise(1)
            )
            
            rate = rate.withColumn(
                'DLY_NUM',
                rate['IN_IS_DLY'] * rate['IN_NUM'] +
                rate['OUT_IS_DLY'] * rate['OUT_NUM']
            ).withColumn(
                'ABS_DLY_DAYS',
                rate['IN_IS_DLY'] * rate['IN_NUM'] * rate['IN_DLY_DAYS'] + 
                rate['OUT_IS_DLY'] * rate['OUT_NUM'] * rate['OUT_DLY_DAYS']
            )
            
            rate = rate.withColumn(
                'INTIME_RATE',
                1 - rate['DLY_NUM'] / rate['采购数量']
            ).withColumn(
                'AVG_DLY_DAYS',
                fun.when(
                    rate['DLY_NUM'] > 0,
                    rate['ABS_DLY_DAYS'] / rate['DLY_NUM']
                ).otherwise(0)
            )
            
            rate = rate.groupBy('ORD_ID') \
                .agg(
                    fun.first('采购组织').alias('采购组织'),
                    fun.first('供应商').alias('供应商'),
                    fun.first('采购订单编号').alias('采购订单编号'),
                    fun.first('工程号').alias('工程号'),
                    fun.first('采购员').alias('采购员'),
                    fun.first('采购日期').alias('采购日期'),
                    fun.first('助记码').alias('助记码'),
                    fun.first('物料名称').alias('物料名称'),
                    fun.first('规格型号').alias('规格型号'),
                    fun.first('存货类别').alias('存货类别'),
                    fun.first('计划到货日期').alias('计划到货日期'),
                    fun.last('入库创建日期').alias('入库创建日期'),
                    fun.first('采购数量').alias('采购数量'),
                    fun.last('IN_SUM').alias('IN_SUM'),
                    
                    fun.avg('INTIME_RATE').alias('INTIME_RATE'),
                    fun.sum('ABS_DLY_DAYS').alias('ABS_DLY_DAYS'),
                    fun.avg('AVG_DLY_DAYS').alias('AVG_DLY_DAYS')
                )
            
            rate = rate.groupBy(
                    '供应商','采购订单编号','助记码'
                ).agg(
                    fun.first('ORD_ID').alias('采购单据ID'),
                    fun.first('采购组织').alias('采购组织'),
                    fun.first('采购员').alias('采购员'),
                    fun.first('工程号').alias('工程号'),
                    fun.first('采购日期').alias('采购日期'),
                    fun.first('物料名称').alias('物料名称'),
                    fun.first('规格型号').alias('规格型号'),
                    fun.first('存货类别').alias('存货类别'),
                    fun.last('计划到货日期').alias('计划到货日期'),
                    fun.last('入库创建日期').alias('入库创建日期'),
                    
                    fun.sum('采购数量').alias('采购总数'),
                    fun.sum('IN_SUM').alias('入库总数'),
                    (fun.sum('IN_SUM') / fun.sum('采购数量')).alias('完成率'),
                    fun.avg('INTIME_RATE').alias('及时率'),
                    fun.sum('ABS_DLY_DAYS').alias('绝对延期天数'),
                    fun.avg('AVG_DLY_DAYS').alias('平均延期天数')
                )
            rate.write.json(self.hdfs_out)
        except Exception as err:
            ApiConf().get_file_logger('spark').error(err)
            self.client.delete(self.hdfs_in, recursive=True)
            self.client.delete(self.hdfs_out, recursive=True)
            spark.stop()
            del self.client, spark, win_ord, rate
            raise Exception(err)
        
        spark.stop()
        del spark, win_ord, rate
        
