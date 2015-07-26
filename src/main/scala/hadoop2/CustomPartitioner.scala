package hadoop2

import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.io.Writable

class CustomPartitioner extends Partitioner[MaxTempWritable, Writable]{

  override def getPartition(key:MaxTempWritable, value: Writable, numPartitions: Int):Int = {
    Math.abs(key.key.hashCode()) % numPartitions
  }
}
