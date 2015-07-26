package hadoop2

import org.apache.hadoop.io.WritableComparator
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class SortDownComparator(keyClass: Class[_ <: WritableComparable[MaxTempWritable]], createInstances: Boolean)
    extends WritableComparator(keyClass) {

  def this() = {
    this(classOf[MaxTempWritable], false)
  }
  def this(keyClass: Class[_ <: WritableComparable[MaxTempWritable]]) = {
    this(keyClass, false)
  }
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    if (a.isInstanceOf[MaxTempWritable] && b.isInstanceOf[MaxTempWritable]) {
      val oneKey = classOf[MaxTempWritable].cast(a).key
      val anotherKey = classOf[MaxTempWritable].cast(b).key

      // 最初に実際のキーでの比較
      val keyCompare: Int = oneKey.compareTo(anotherKey)
      if (keyCompare != 0) {
        keyCompare
      }
      val oneOrder = classOf[MaxTempWritable].cast(a).value
      val anotherOrder = classOf[MaxTempWritable].cast(b).value
      -oneOrder.compareTo(anotherOrder)
    }
    super.compare(a, b);
  }
}