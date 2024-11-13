REGISTER /usr/hdp/current/phoenix-client/phoenix-client.jar

-- Load the ratings data from HDFS
movies_items = LOAD '/user/root/data/movies_items' 
USING PigStorage('|') 
AS (movie_id:int, movie_title:chararray, release_date:chararray, release_video:chararray, imdb_link:chararray);

-- Store data into HBase using Phoenix
STORE movies_items INTO 'hbase://movies_items' 
USING org.apache.phoenix.pig.PhoenixHBaseStorage('localhost','-batchSize 5000');
