package hadoop

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import java.io.IOException
import collection.JavaConversions.iterableAsScalaIterable

class MaxTemperatureReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  type Context = Reducer[Text, IntWritable, Text, IntWritable]#Context
  override def reduce(
      key: Text,
      values: java.lang.Iterable[IntWritable],
      context: Context) {
      var maxValue = Integer.MIN_VALUE
      for (value: IntWritable <- values) {
        maxValue = Math.max(maxValue, value.get())
      }
      context.write(key, new IntWritable(maxValue))
  }
}

