package hadoop2

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ Tool, ToolRunner }
import org.apache.hadoop.mapreduce.{ Job, Mapper}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.{ IntWritable, Text}
import org.apache.hadoop.fs.Path
import hadoop.MaxTemperatureMapper
import hadoop.MaxTemperatureReducer
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

class MaxTempTop10Driver extends Configured with Tool {
  
  def createMaxTemperatureJob(args: Array[String]): Job = {
    val job = new Job(this.getConf(), "Max temperature")
    job.setJarByClass(this.getClass())

    FileInputFormat.addInputPath(job, new Path(args(0)))

    val sequenceFileOutputFormat = new SequenceFileOutputFormatHelper()
    sequenceFileOutputFormat.setOutputPath(job, new Path(args(1) + "-temp"))
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, IntWritable]]);

    job.setMapperClass(classOf[MaxTemperatureMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    // CombinerのInputとOutputはMapperと同じでないとダメなので指定できない。
    // job.setCombinerClass(classOf[MaxTemperatureReducer2])
    job.setReducerClass(classOf[MaxTemperatureReducer2])
    
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[MaxTempWritable])
    job
  }

  def createTop3Job(args: Array[String]): Job = {
    val job = new Job(this.getConf(), "Max temperature")
    job.setJarByClass(this.getClass())

    val sequenceFileInputFormat = new SequenceFileInputFormatHelper()
    sequenceFileInputFormat.setInputPaths(job, new Path(args(1) + "-temp"));
    job.setInputFormatClass(classOf[SequenceFileInputFormat[Text,IntWritable]]);

    FileOutputFormat.setOutputPath(job, new Path(args(1) + "-top3"))

    job.setMapperClass(classOf[Mapper[_, _, _, _]]);
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[MaxTempWritable])

//    job.setCombinerClass(classOf[Top3Reducer])
    job.setReducerClass(classOf[Top3Reducer])

    // ここ問題有り.解決すれば終了
 //   job.setGroupingComparatorClass(classOf[CustomGroupingComparator])
   // job.setPartitionerClass(classOf[CustomPartitioner])
    job.setSortComparatorClass(classOf[SortDownComparator])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job
  }

  override def run(args: Array[String]): Int = {
    if (args.length != 2) {
      println("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName())
      ToolRunner.printGenericCommandUsage(System.err)
      -1
    }
    
    val jobs: List[Job] = List(
        createMaxTemperatureJob(args),
        createTop3Job(args))

    for (job: Job <- jobs) {
      if (!job.waitForCompletion(true)) 1
    }
    0
  }
}

object MaxTempTop10Driver {
  def main(args: Array[String]) {
    val exitCode = ToolRunner.run(new MaxTempTop10Driver(), args)
    System.exit(exitCode)
  }
}
