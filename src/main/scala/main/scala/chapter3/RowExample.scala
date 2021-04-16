package main.scala.chapter3

import org.apache.spark.sql.Row
object RowExample {

    def main(args: Array[String]): Unit = {
        // Create a Row
        val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))
        // Access using index for individual items
        for (i <- 0 until blogRow.size) {
            if (blogRow(i).isInstanceOf[Array[String]]) {
                println(blogRow(i).asInstanceOf[Array[String]].mkString("{", ",", "}"))
            } else {
                println(blogRow(i))
            }
        }

    }
}
