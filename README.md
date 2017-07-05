# Description

## Data and result in the resources directory

### 1, input data dir

> resource/data/wine

### 2, center and result
> means_3, 3 means k = 3, centers.txt is the center data, and result.txt is the result, and means_5,means_8, means_10 are the same


### 3, file tree in hdfs
> hadoop@hadoop702:~$ hadoop fs -ls /user/data

> -rwxrwxrwx   3 hadoop hadoop         29 2017-07-05 18:49 /user/data/output.txt
> drwxrwxrwx   - hadoop hadoop          0 2017-07-05 18:49 /user/data/win

### 4, how to compile to jar package 

> open this project in idea and execute the 'mvn clean pacakge' in the project root dir, you will get jar package