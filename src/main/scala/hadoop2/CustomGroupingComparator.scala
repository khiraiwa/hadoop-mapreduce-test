package hadoop2

import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.WritableComparator

class CustomGroupingComparator(keyClass: Class[_ <: WritableComparable[_]], createInstances: Boolean) extends WritableComparator(keyClass, createInstances) {

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    if (a.isInstanceOf[MaxTempWritable] && b.isInstanceOf[MaxTempWritable]) {
      val one = classOf[MaxTempWritable].cast(a).key
      val another = classOf[MaxTempWritable].cast(b).key
      return one.compareTo(another)
    }
    super.compare(a, b)
  }
}