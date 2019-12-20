# MapReduce
This repository contains many programs from Big Data field using mapreduce written in Java, with as simple code as possible.

# In order to run all these programs successfully, requirements: -
An input file which should be present in HDFS.
Jar file of each program to run that respective program, create that by exporting your project as an jar file.

# Command to copy a file from local filesystem to HDFS: -
hdfs dfs -put <[filename]>
  
# Command to execute program: -
1. hadoop jar <jar file name>.jar <[Driver Class Name]> <[Input File Name]> <[Output Directory]>   (Make sure that the output directory do not exists previosuly, otherwise command will fail.)
2. yarn jar <[jar file name]>.jar <[Driver Class Name]> <[Input File Name]> <[Output Directory>
  
# To check the output: -
hdfs dfs -cat <[output directory name]>/p*
