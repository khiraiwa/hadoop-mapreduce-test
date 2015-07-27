package hadoop2

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import java.io.IOException
import collection.JavaConversions.iterableAsScalaIterable
import scala.util.control.Breaks.{ break, breakable }
import java.util.Collection

class Limit3Reducer extends Reducer[Text, MaxTempWritable, Text, IntWritable] {
  type Context = Reducer[Text, MaxTempWritable, Text, IntWritable]#Context
  override def reduce (
      key: Text,
      values: java.lang.Iterable[MaxTempWritable],
      context: Context) {

    var cnt = 0
    breakable {
      for (w: MaxTempWritable <- values) {
       // if (cnt >= 3) {
       //   break
        //}
        context.write(w.key, w.value)
        cnt += 1
      }
    }
  }
}

