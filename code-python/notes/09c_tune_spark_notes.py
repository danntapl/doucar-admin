# ## Tuning Spark Applications


# ## Manual timings 

# I (Glynn) have tested this script on the cluster1.duocar.us cluster
# several times, informally using a stopwatch.  

# This cluster has 3 worker nodes, with 2 YARN vcores each, so the maximum 
# number of executors available for one run is 5.  A simple rule of thumb
# for repartitioning larger DataFrames is max available parallelism x 1.9, 
# so, 5 x 1.9 (rounded down in this case) = 9. 

# Here are timings obtained:

# Run 1  Local run                 3:50  

# Run 2  YARN                      3:38

# Run 3  YARN, 
#        repartition(9)            1:47

# Run 4  YARN, 
#        repartition(9), 
#        persist(...MEMORY_ONLY)   1:02

# Run 5  Exercise:
#        local[2],
#        repartition(9),
#        persist(...MEMORY_ONLY)   0:56

# ## Some intuitions

#**Note 1**
#Run 2 achieves some speed-up because of the parallelism of 5 executors
#running on the cluster.  This is somewhat penalized by the higher startup
#costs on the cluster.  You'd expect the contrast between Run 1 and Run 2 
#to be greater if the datesets involved were much larger and/or the cluster
#was larger, affording more parallism.  
#
#**Note 2**
#Why is Run 5 (local, repartitioning, caching) even faster than Run 4, 
#run on YARN.  
#1. As with Note 1, the local run experiences far lower startup cost than
#a run on the cluster.  
#2. Open the Spark UI and look at the Storage tab: the cached
#DataFrame is only 60.9KB, which is readily handled in the local container.
#If the dataset(s) to be cached were much larger, we'd likely see the 
#distributed run excel because it would have more (distributed) cache space
#available.