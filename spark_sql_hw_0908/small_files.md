# 如何避免小文件问题?

1. Coalesce

2. Repartition
   
df.repartition(100). In this case, a round-robin partitioner is used, meaning the only guarantee is that the output data has roughly equally sized sPartitions.

3. Range Partition

4. Compact小文件。

通过设置自动合并文件的参数，如文件个数小于多少的文件数到达一定阈值。也可以通过手动的Compact命令触发。类似于HBase里的Minor Compaction和Major Compaction。如果自动触发，像HBase Major Compaction那样，合并文件过多时可能会占用过多系统资源。因此如果写入时能自动监测到产生的小文件数量超过一定阈值，在写入时即可立即合并判断可以降低一次性合并过多文件造成的系统资源消耗峰值。自动触发的合并规则，如合并文件数，合并文件大小，下一个加入合并队列的文件大小选择；如何控制compaction操作造成的频繁写入导致写速度的下降，是否需要避免写放大导致的反复读取与写入，要根据具体环境制定合适策略。
 
