hdfs dfs -mkdir /root
hdfs dfs -rm /root/test2d.csv
hdfs dfs -rm /root/test3d.csv
hdfs dfs -rm /root/random_32.csv
hdfs dfs -rm /root/random_64.csv


hdfs dfs -put inputs/test2d.csv /root
hdfs dfs -put inputs/test3d.csv /root
hdfs dfs -put inputs/random_32.csv /root
hdfs dfs -put inputs/random_64.csv /root