from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from pyspark.sql.functions import *
from pyspark.sql.types import *

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import logging


pathProvenanceHIL = '/home/vieira/ProjectStreamingHIL/ProvenanceHIL/'
pathResultsPreview = '/home/vieira/ProjectStreamingHIL/ResultsPreview/'

provenanceHIL = logging.getLogger('myapp')
hdlrHIL = logging.FileHandler(pathProvenanceHIL+'provenanceHIL.csv')
formatterHIL = logging.Formatter('%(asctime)s ; %(message)s')
hdlrHIL.setFormatter(formatterHIL)
provenanceHIL.addHandler(hdlrHIL) 
provenanceHIL.setLevel(logging.INFO)

resultsPreview = logging.getLogger('myapp1')
hdlrPreview = logging.FileHandler(pathResultsPreview+'resultsPreview.csv')
formatterPreview = logging.Formatter('%(asctime)s ; %(message)s')
hdlrPreview.setFormatter(formatterPreview)
resultsPreview.addHandler(hdlrPreview)
resultsPreview.setLevel(logging.INFO)


class MyHandler(PatternMatchingEventHandler):

    patterns = ['*.sql','*.csv']

    def process(self, event):

        if event.event_type == 'created':

            global codeActivity_1, codeActivity_2, codeActivity_3
            global idActivity_1, idActivity_2, idActivity_3 
            
            newEvent = open(event.src_path).read()

            if '/Activity_1/' in  event.src_path:
                spark.streams.get(idActivity_1).stop()
                
                new_activity_1 = spark.sql(newEvent) \
                .writeStream \
                .format('csv') \
                .trigger(processingTime="10 seconds") \
                .option("checkpointLocation", pathCheckpoint_1) \
                .option("path", pathStreamingSink_1) \
                .start() 

                provenanceHIL.info('activity_1 ; ' + codeActivity_1 + '; ' + newEvent)

                idActivity_1 = new_activity_1.id
                codeActivity_1 = newEvent
		
            if '/Activity_2/' in  event.src_path:
                spark.streams.get(idActivity_2).stop()
               
                new_activity_2 = spark.sql(newEvent) \
                .writeStream \
                .format('csv') \
                .trigger(processingTime="10 seconds") \
                .option("checkpointLocation", pathCheckpoint_2) \
                .option("path", pathStreamingSink_2) \
                .start() 

                provenanceHIL.info('activity_2 ; ' + codeActivity_2 + '; ' + newEvent)

                idActivity_2 = new_activity_2.id
                codeActivity_2 = newEvent

            if '/Activity_3/' in  event.src_path:
                spark.streams.get(idActivity_3).stop()
                
                new_activity_3 = spark.sql(newEvent) \
                .writeStream \
                .format('csv') \
                .trigger(processingTime="10 seconds") \
                .option("checkpointLocation", pathCheckpoint_3) \
                .option("path", pathStreamingSink_3) \
                .start() 

                provenanceHIL.info('activity_3 ; ' + codeActivity_3 + '; ' + newEvent)

                idActivity_3 = new_activity_3.id
                codeActivity_3 = newEvent

            if '/StreamingSink_3/' in event.src_path:
                if newEvent: 
                    resultsPreview.info(newEvent)
		

    def on_created(self, event):
        self.process(event)


if __name__ == '__main__':

    spark = SparkSession.builder \
        .master('local') \
        .appName('streamingHIL') \
        .getOrCreate()

    SparkSession.builder.config(conf=SparkConf())


    pathActivity_1 = '/home/vieira/ProjectStreamingHIL/Dataflow/Activity_1/'
    pathActivity_2 = '/home/vieira/ProjectStreamingHIL/Dataflow/Activity_2/'
    pathActivity_3 = '/home/vieira/ProjectStreamingHIL/Dataflow/Activity_3/'
    pathCheckpoint_1 = '/home/vieira/ProjectStreamingHIL/Checkpoint_1/'
    pathCheckpoint_2 = '/home/vieira/ProjectStreamingHIL/Checkpoint_2/'
    pathCheckpoint_3 = '/home/vieira/ProjectStreamingHIL/Checkpoint_3/'
    pathStreamingInput ='/home/vieira/ProjectStreamingHIL/StreamingInput/' 
    pathStreamingSink_1 = '/home/vieira/ProjectStreamingHIL/StreamingSink_1/'
    pathStreamingSink_2 = '/home/vieira/ProjectStreamingHIL/StreamingSink_2/'
    pathStreamingSink_3 = '/home/vieira/ProjectStreamingHIL/StreamingSink_3/'
    

    schema = StructType([StructField('date_time', StringType(), True),\
                          StructField('temperature', FloatType(), True),\
                          StructField('pressure', FloatType(), True)])



    #------------------------------------------Original dataflow---------------------------------------------------------------------

    #Streaming queries = dataflow activities
      
    rawInput = spark.readStream.csv(pathStreamingInput,header=True,schema=schema)
    rawInput.createOrReplaceTempView('rawInput')

    codeActivity_1 = open(pathActivity_1+'activity_1.sql').read()
    
    activity_1 = spark.sql(codeActivity_1) \
      .writeStream \
      .format('csv') \
      .trigger(processingTime="10 seconds") \
      .option("checkpointLocation", pathCheckpoint_1) \
      .option("path", pathStreamingSink_1) \
      .start() 

    idActivity_1 = activity_1.id


    inputActivity_2 = spark.readStream.csv(pathStreamingSink_1,schema=schema)
    inputActivity_2.createOrReplaceTempView('inputActivity_2')
    
    codeActivity_2 = open(pathActivity_2+'activity_2.sql').read()

    activity_2 = spark.sql(codeActivity_2)\
      .writeStream \
      .format('csv') \
      .trigger(processingTime="10 seconds") \
      .option("checkpointLocation", pathCheckpoint_2) \
      .option("path", pathStreamingSink_2) \
      .start() 

    idActivity_2 = activity_2.id


    inputActivity_3 = spark.readStream.csv(pathStreamingSink_2,schema=schema)
    inputActivity_3.createOrReplaceTempView('inputActivity_3')

    codeActivity_3 = open(pathActivity_3+'activity_3.sql').read()

    activity_3 = spark.sql(codeActivity_3) \
      .writeStream \
      .format('csv') \
      .trigger(processingTime="10 seconds") \
      .option("checkpointLocation", pathCheckpoint_3) \
      .option("path", pathStreamingSink_3) \
      .start() 

    idActivity_3 = activity_3.id


#---------------------------------------------------End dataflow-------------------------------------------------------------------------------

    observer_1 = Observer()
    observer_1.schedule(MyHandler(), path= pathActivity_1)
    observer_1.start()

    observer_2 = Observer()
    observer_2.schedule(MyHandler(), path= pathActivity_2)
    observer_2.start()

    observer_3 = Observer()
    observer_3.schedule(MyHandler(), path= pathActivity_3)
    observer_3.start()

    observer_4 = Observer()
    observer_4.schedule(MyHandler(), path= pathStreamingSink_3)
    observer_4.start()


    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer_1.stop()
        observer_2.stop()
        observer_3.stop()
        observer_4.stop()       

    observer_1.join()
    observer_2.join()
    observer_3.join()
    observer_4.join()


