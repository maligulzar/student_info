import java.util.Calendar
import org.apache.spark.lineage.LineageContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 4/20/16.
 */
object SequentialSplit {
	def main(args: Array[String]): Unit = {
		//set up logging

		//set up spark configuration
		val sparkConf = new SparkConf().setMaster("local[6]")
		sparkConf.setAppName("Student_Info")
			.set("spark.executor.memory", "2g")

		//set up spark context
		val ctx = new SparkContext(sparkConf)


		//set up lineage context
		val lc = new LineageContext(ctx)
		lc.setCaptureLineage(true)

		//spark program starts here
		val records = lc.textFile("src/patientData.txt1", 1)
		var weights = List[Double]()
		weights = 3.0d :: weights
		weights = 2.0d :: weights
		weights = 5.0d :: weights
		split(weights, records, records.count)(0).collect().foreach(println)
	}

	def split(w: List[Double], rdd: RDD[_], count: Double) = {
		val zipped = rdd.zipWithIndex()
		val sum = w.reduce(_ + _)
		val sumweights = w.map(_ / sum).scanLeft(0.0d)(_ + _)
		val rddlist = sumweights.sliding(2).map { x =>
			zipped.filter { y =>
				val in = y._2.toDouble / count
				x(0) <= in && in < x(1)
			}.map(x => x._1)
		}
		rddlist.toList
	}
}