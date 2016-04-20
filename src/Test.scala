/**
 * Created by Michael on 1/25/16.
 */

import java.io._
import java.util.logging.{FileHandler, Level, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Test extends userTest[String] with Serializable {
	var num = 0;

	def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		val logger: Logger = Logger.getLogger(classOf[Test].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass, which returns false
		var returnValue = false
		val finalRdd = inputRDD.map(line => {
			val list = line.split(" ")
			(list(5), list(3).toInt)
		}).groupByKey
			.map(pair => {
			val itr = pair._2.toIterator.asInstanceOf[Iterator[(Int,Long)]]
			var moving_average = 0.0
			var num = 1
			while (itr.hasNext) {
				moving_average = moving_average + (itr.next()._1 - moving_average) / num
				num = num + 1
			}
			(pair._1, moving_average)
		})
		val start = System.nanoTime

		val out = finalRdd.collect()

		logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000 + "")
		num = num + 1
		logger.log(Level.INFO, "TestRuns :" + num + "")
		println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>   Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<""")
		for (o <- out) {
			// println(o)
			if (o._2.asInstanceOf[Float] > 25) {
				returnValue = true
			}
		}
		returnValue
	}
}
