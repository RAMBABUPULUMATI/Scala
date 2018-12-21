import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object SparkWordCount {
   def main(args: Array[String]) {

    val sc = new SparkContext( "local", "Word Count", "/usr/svchdphdo4ecfd", Nil, Map(), Map())           
      val input = sc.textFile("input.txt")             
      Val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)       
      count.saveAsTextFile("outfile")
      System.out.println("OK");
   }
}