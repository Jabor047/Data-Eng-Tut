from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import when


class SplitData:
    def __init__(self):
        pass
    def split_data(self, filename):
        spark = SparkSession.builder.appName("DataFrame").getOrCreate()

        # The schema is encoded in a string.
        schemaList = ['utc_time_id', 'source_id', 'feed_id', 'primary_link_source_flag',
                        'samples', 'avg_speed','avg_flow','avg_occ','avg_freeflow_speed', 
                        'avg_travel_time', 'high_quality_samples', 'samples_below_100pct_ff',
                        'samples_below_95pct_ff', 'samples_below_90pct_ff', 'samples_below_85pct_ff',
                        'samples_below_80pct_ff', 'samples_below_75pct_ff', 'samples_below_70pct_ff',
                        'samples_below_65pct_ff', 'samples_below_60pct_ff', 'samples_below_55pct_ff',
                        'samples_below_50pct_ff', 'samples_below_45pct_ff', 'samples_below_40pct_ff',
                        'samples_below_35pct_ff', 'samples_below_30pct_ff']

        # here i am converting all 255 column to stringtype since this is just for loading sake. to define the mysql 
        # schema you have to pass the correct datatype
        fields = [StructField(field_name, StringType(), True) for field_name in schemaList]
        schema = StructType(fields)

        df = spark.read.csv(filename, header=False, schema=schema).toDF ('utc_time_id', 'source_id', 'feed_id', 'primary_link_source_flag',
            'samples', 'avg_speed','avg_flow','avg_occ','avg_freeflow_speed', 
            'avg_travel_time', 'high_quality_samples', 'samples_below_100pct_ff',
            'samples_below_95pct_ff', 'samples_below_90pct_ff', 'samples_below_85pct_ff',
            'samples_below_80pct_ff', 'samples_below_75pct_ff', 'samples_below_70pct_ff',
            'samples_below_65pct_ff', 'samples_below_60pct_ff', 'samples_below_55pct_ff',
            'samples_below_50pct_ff', 'samples_below_45pct_ff', 'samples_below_40pct_ff',
            'samples_below_35pct_ff', 'samples_below_30pct_ff'
            )
        # Drop columns having Null values to all the records
        new_df = df.drop('samples_below_85pct_ff',
            'samples_below_80pct_ff', 'samples_below_75pct_ff', 'samples_below_70pct_ff',
            'samples_below_65pct_ff', 'samples_below_60pct_ff', 'samples_below_55pct_ff',
            'samples_below_50pct_ff', 'samples_below_45pct_ff', 'samples_below_40pct_ff',
            'samples_below_35pct_ff', 'samples_below_30pct_ff')


        clean_df = SplitData.filter_null(self,new_df)
        clean_df = SplitData.filter_zeros(self,clean_df)
        
        #clean_df= clean_df.orderBy('utc_time_id')
        #clean_df = clean_df.na.fill(0)
        #clean_df.write.csv('clean.csv')
        clean_df.printSchema()
        return clean_df

        
    def filter_zeros (self, df):
        """
         Function that filters the row contains zero for one third of the columns 
    
        """
        
        df1 = df.withColumn('Result',
        when(df.primary_link_source_flag!='0',"True").
        when(df.samples!='0',"True").
        when(df.avg_speed!='0',"True").
        when(df.avg_flow!='0',"True").
        when(df.avg_occ!='0',"True").
        when(df.avg_freeflow_speed !='0',"True").
        when(df.avg_travel_time !='0',"True").
        when(df.high_quality_samples !='0',"True").

        when(df.samples_below_100pct_ff!='0',"True").
        when(df.samples_below_95pct_ff !='0',"True").
        when(df.samples_below_90pct_ff !='0',"True")
        
        ).filter("Result==True").drop("Result")
        clean_df = SplitData.filter_null(self,df1)
        clean_df= clean_df.orderBy('utc_time_id')
        clean_df.write.csv('small.csv')



        return df
    

    def filter_null (self, df):
        """
        Function that filter null values
        """
        df1 = df.withColumn('Result',
        when(df.primary_link_source_flag!='0',"True").
        when(df.samples!='null',"True").
        when(df.avg_speed!='null',"True").
        when(df.avg_flow!='null',"True").
        when(df.avg_occ!='null',"True").
        when(df.avg_freeflow_speed !='null',"True").
        when(df.avg_travel_time !='null',"True").
        when(df.high_quality_samples !='null',"True").

        when(df.samples_below_100pct_ff!='null',"True").
        when(df.samples_below_95pct_ff !='null',"True").
        when(df.samples_below_90pct_ff !='null',"True")
        
        ).filter("Result==True").drop("Result")

        return df1



if __name__=="__main__":
    obj_split_data = SplitData()
    obj_split_data.split_data("I80_davis.txt")
