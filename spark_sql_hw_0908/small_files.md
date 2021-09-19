# 如何避免小文件问题?

1. Coalesce

2. Repartition
   
df.repartition(100). In this case, a round-robin partitioner is used, meaning the only guarantee is that the output data has roughly equally sized sPartitions.

3. Range Partition

4. Compact Table

5. 
