package hadoop

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import java.io.IOException
import org.apache.hadoop.mrunit.mapreduce.MapDriver

class MaxTemperatureMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  type Context = Mapper[LongWritable, Text, Text, IntWritable]#Context
  override def map(
      key: LongWritable,
      value: Text,
      context: Context) {
    val line = value.toString()
    val year = line.substring(
        MaxTemperatureMapper.YEAR_BEGIN_INDEX,
        MaxTemperatureMapper.YEAR_END_INDEX)
    val temp = line.substring(
        MaxTemperatureMapper.TEMP_BEGIN_INDEX,
        MaxTemperatureMapper.TEMP_END_INDEX)
    if (!missing(temp)) {
      val airTemperature = Integer.parseInt(temp)
      context.write(new Text(year), new IntWritable(airTemperature))
    }
  }

  def missing(temp: String):Boolean = {
    return temp eq "+9999"
  }
}

object MaxTemperatureMapper {
  private val YEAR_BEGIN_INDEX = 15
  private val YEAR_END_INDEX = 19
  private val TEMP_BEGIN_INDEX = 87
  private val TEMP_END_INDEX = 92
}