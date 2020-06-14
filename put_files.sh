hdfs dfs -mkdir /root
hdfs dfs -rm /root/test2d.csv
hdfs dfs -rm /root/test3d.csv


hdfs dfs -put inputs/test2d.csv /root
hdfs dfs -put inputs/test3d.csv /root