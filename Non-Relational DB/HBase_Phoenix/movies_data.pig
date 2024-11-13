REGISTER /usr/hdp/current/phoenix-client/phoenix-client.jar

-- Load the ratings data from HDFS
movies_data = LOAD '/user/root/data/movies_data' 
USING PigStorage('\t') 
AS (userID:int, movieID:int, rating:int, rating_time:long);

-- Store data into HBase using Phoenix
STORE movies_data INTO 'hbase://movies_data' 
USING org.apache.phoenix.pig.PhoenixHBaseStorage('localhost','-batchSize 5000');
