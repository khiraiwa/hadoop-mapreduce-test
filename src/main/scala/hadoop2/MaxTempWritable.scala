package hadoop2

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.WritableComparable

class MaxTempWritable(
    _key: Text,
    _temp: IntWritable
//     ) extends Writable with WritableComparable[MaxTempWritable] {
         ) extends WritableComparable[MaxTempWritable] {
  private val k: Text = _key
  private val temp: IntWritable = _temp

  def key = k
  def value = temp

  def this() {
    this(new Text("aaa"), new IntWritable(11))
  }

  override def write(out: DataOutput) {
    key.write(out)
    temp.write(out)
  }

  override def readFields(in: DataInput) {
    key.readFields(in)
    temp.readFields(in)
  }
  override def compareTo(writable: MaxTempWritable): Int = {
    this.value.get().compareTo(writable.value.get())
  }
}
