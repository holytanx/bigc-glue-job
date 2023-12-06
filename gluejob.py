from awsglue import DynamicFrame
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from corelib.entry_point import main
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, minute, year, month, col, dayofmonth, to_utc_timestamp
from awsglue.dynamicframe import DynamicFrame
import pytz

utc = 'UTC'
thai = 'Asia/Bangkok'

class CustomGlueJob:
    def __init__(self):
        params = []
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
        args = getResolvedOptions(sys.argv, params)

        self.sc = SparkContext.getOrCreate()
        # self.sc.setLogLevel('DEBUG')
        self.context = GlueContext(self.sc)
        self.job = Job(self.context)

        if 'JOB_NAME' in args:
            jobname = args['JOB_NAME']
        else:
            jobname = "test"
        self.job.init(jobname, args)
        self.logger = print_logger() if jobname =="test" else self.context.get_logger()

    def run(self):
        self.logger.info("Step 1. Read raw messages from glue table")
        df = read_table(self.context, "bigc", "raw");
        raw_msg_list = df.toJSON().map(lambda j: json.loads(j)).collect()
        self.logger.info("Total Count: " + str(len(raw_msg_list)))
        raw_msg_list = [msg for msg in raw_msg_list if not '_corrupt_record' in msg]
        self.logger.info("Total Good record Count: " + str(len(raw_msg_list)))

        self.logger.info("----------------------------------------------------------")
        
        self.logger.info("Step 2. Transform message object to expected format object")
        
        self.logger.info("2.1 Get necessary data (channel list, dict of each user interaction messsage list)")
        channel_list = get_channel_list(raw_msg_list)
        dict_of_per_user_intr_msg_lst = get_per_user_interact_msg_list(raw_msg_list)
        flow_detection_regex_lst = get_flow_detection_regex()
        
        self.logger.info("2.2 Start transforming data")
        result = get_formatted_dict_of_per_user_msg(channel_list, flow_detection_regex_lst, dict_of_per_user_intr_msg_lst, self.logger)
                
        rdd =  self.sc.parallelize(result)
        df = rdd.toDF()
        df = df.withColumn("year", year(col("utc_timestamp")))\
          .withColumn("month", month(col("utc_timestamp")))\
          .withColumn("day", dayofmonth(col("utc_timestamp")))\
          .withColumn("hour", hour(col("utc_timestamp")))\
          .withColumn("min", minute(col("utc_timestamp")))
          
        df = df.withColumn("thai_timestamp", to_utc_timestamp(col("thai_timestamp"), thai))
        dyf = DynamicFrame.fromDF(df, self.context, "result")
        
        self.logger.info("----------------------------------------------------------")
        
        self.logger.info("Step 3. Transfer to S3")
        write_to_s3(self.context, dyf, "s3://bigc-data-pipeline-dev/processed/")

        self.job.commit()


'''
        {
            '<user_id_1>': [
                {message from user 1 to bot},
                {message from bot to user 1},
                ....
            ],
            <user_id_2>: [
                ...
            ],
            ...
        }
 '''
def get_per_user_interact_msg_list(msg_list):
    dict_of_per_user_intr_msg_list = {}
    for msg in msg_list:
        user_id = msg['recipient_id'] if msg['source'] == 'bot' else msg['sender_id']
        if user_id not in dict_of_per_user_intr_msg_list:
            dict_of_per_user_intr_msg_list[user_id] = [msg]
        else:
            prev_msg_list = dict_of_per_user_intr_msg_list[user_id]
            dict_of_per_user_intr_msg_list[user_id] = prev_msg_list + [msg]
    return dict_of_per_user_intr_msg_list
        
        

'''
        [
            {
                channel_id: '....',
                channel_type: 'FACEBOOK'
            },
            ....
        ]
'''
def get_channel_list(msg_list):
    
    all_ch = []
    all_ch_type = []
    # Get bot messages <- Use for get channel info
    bot_msg_list = [msg for msg in msg_list if 'source' in msg and msg['source'] == 'bot']    
    
    # Find all channels
    for msg in bot_msg_list:
        ch_type = msg['channel_type']
        ch_id = msg['channel_id']
        if ch_type not in all_ch_type:
            
            all_ch.append(
                {
                    'channel_id': ch_id,
                    'channel_type': ch_type
                }
            )
            all_ch_type.append(ch_type)
    
    return all_ch        
    

def get_formatted_dict_of_per_user_msg(channel_list, flow_detection_regex_lst, user_message_list, logger):
    return main(channel_list, flow_detection_regex_lst, user_message_list, logger)
    


def read_table(glue_context, db_name, tb_name):
  #datasource0
  dynamicframe = glue_context.create_dynamic_frame.from_catalog(database=db_name,\
  table_name=tb_name, transformation_ctx = 'datasource1', additional_options = {"useSparkDataSource": True}).toDF()
  return dynamicframe


def write_to_s3(glue_context, dyf, s3_path):
    glue_context.write_dynamic_frame \
    .from_options(
        frame = dyf, 
        connection_type = "s3", 
        connection_options =  {"path": s3_path, "partitionKeys": ["year", "month", "day", "hour", "min"]},
        format = "parquet")

def get_flow_detection_regex():
    f = open ('flow_detection_regex.json', "r")
    json_object = json.loads(f.read())
    return json_object

class print_logger:
    def __init__(self):
        pass
    
    def info(self, msg):
        print(msg)


CustomGlueJob().run()