# Spark-DNAligning

The evolution of technologies has unleashed a wealth of challenges by generating massive amount of data. Recently, biological data has increased exponentially, which has introduced several computational challenges. DNA short read alignment is an important problem in bioinformatics. The exponential growth in the number of short reads has increased the need for an ideal platform to accelerate the alignment process. Apache Spark is a cluster-computing framework that involves data parallelism and fault tolerance. Spark-DNAligning is a Spark-based algorithm to accelerate DNA short reads alignment problem.

# How to use it?
1. Prepare the DNA reference file by running `toLine.py` script. You can find it in `helper_scripts` folder.

2. Compress the short reads file by running:
```
bzip2 short_reads_file_name
```

3. Create an Amazon S3 bucket. Upload the following files to it:
  - The DNA reference file.
  - The short reads file.
  - The jar file.

4. The jar file and the DNA reference file need to be saved on your Amazon [EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview-arch.html) cluster. So, download the content of your Amazon S3 bucket by running the following command on your Amazon EMR cluster:
```
aws s3 cp s3://path/to/your/s3/bucket . --recursive
```
  - Note that the short reads file needs to be on S3 bucket and there is no need to download it to your Amazon EMR cluster.

5. Start Spark-DNAligning by running the following command:
```
spark-submit --class com.ku.Aligning.DNACluster --driver-memory 4g --executor-memory 4g --executor-cores 3 --num-executors 3 dna.jar 16 36 /home/hadoop/s_suisLine.fa path/to/your/s3/bucket/100kGood.fa.bz2 path/to/your/s3/bucket/ Streptococcus_suis
```
