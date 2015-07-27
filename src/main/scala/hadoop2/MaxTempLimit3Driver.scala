package hadoop2

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ Tool, ToolRunner }
import org.apache.hadoop.mapreduce.{ Job, Mapper}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.{ IntWritable, Text}
import org.apache.hadoop.fs.Path
import hadoop.MaxTemperatureMapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

class MaxTempLimit3Driver extends Configured with Tool {
  
  def createMaxTempJob(args: Array[String]): Job = {
    val job = new Job(this.getConf(), "Max temperature")
    job.setJarByClass(this.getClass())

    FileInputFormat.addInputPath(job, new Path(args(0)))

    val sequenceFileOutputFormat = new SequenceFileOutputFormatHelper()
    sequenceFileOutputFormat.setOutputPath(job, new Path(args(1) + "-max"))
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, MaxTempWritable]]);

    job.setMapperClass(classOf[MaxTemperatureMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setReducerClass(classOf[MaxTemperatureReducer2])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[MaxTempWritable])
    job
  }

  def createLimit3Job(args: Array[String]): Job = {
    val job = new Job(this.getConf(), "Limit 3 max temperature")
    job.setJarByClass(this.getClass())

    val sequenceFileInputFormat = new SequenceFileInputFormatHelper()
    sequenceFileInputFormat.setInputPaths(job, new Path(args(1) + "-max"));
    job.setInputFormatClass(classOf[SequenceFileInputFormat[Text,MaxTempWritable]]);

    FileOutputFormat.setOutputPath(job, new Path(args(1) + "-limit3"))

    job.setMapperClass(classOf[Mapper[_, _, _, _]]);
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[MaxTempWritable])

    job.setReducerClass(classOf[Limit3Reducer])
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
    val jobs: List[Job] = List(createMaxTempJob(args), createLimit3Job(args))
    for (job: Job <- jobs) {
      if (!job.waitForCompletion(true)) 1
    }
    0
  }
}

object MaxTempLimit3Driver {
  def main(args: Array[String]) {
    val exitCode = ToolRunner.run(new MaxTempLimit3Driver(), args)
    System.exit(exitCode)
  }
}
