package hadoop

import org.junit.Test
import org.hamcrest.Matchers._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import java.util.Arrays
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver

class JUnitTest {
  @Test
  def processValidRecord(): Unit = {
    val value: Text = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + // 年 ^^^^
	    "99999V0203201N00261220001CN9999999N9-00111+99999999999"); // 気温 ^^^^^
    val test10 = new MaxTemperatureMapper()

    new MapDriver[LongWritable, Text, Text, IntWritable]()
      .withMapper(test10)
      .withInputValue(value)
      .withOutput(new Text("1950"), new IntWritable(-11)) .runTest();
  } 
  
  
  @Test
  def returnsMaximumIntegerInValues():Unit = {
    new ReduceDriver[Text, IntWritable, Text, IntWritable]()
      .withReducer(new MaxTemperatureReducer())
      .withInputKey(new Text("1950"))
      .withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
      .withOutput(new Text("1950"), new IntWritable(10))
      .runTest();
    }

}