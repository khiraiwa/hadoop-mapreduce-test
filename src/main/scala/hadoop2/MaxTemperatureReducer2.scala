package hadoop2

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import java.io.IOException
import collection.JavaConversions.iterableAsScalaIterable

class MaxTemperatureReducer2 extends Reducer[Text, IntWritable, Text, MaxTempWritable] {
  type Context = Reducer[Text, IntWritable, Text, MaxTempWritable]#Context
  override def reduce(
      key: Text,
      values: java.lang.Iterable[IntWritable],
      context: Context) {
      var maxValue = Integer.MIN_VALUE
      for (value: IntWritable <- values) {
        maxValue = Math.max(maxValue, value.get())
      }
      context.write(new Text("key"), new MaxTempWritable(key, new IntWritable(maxValue)))
  }
}

