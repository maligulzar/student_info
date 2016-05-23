/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext._
import org.apache.spark.lineage.rdd.ShowRDD
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.sys.process._
import scala.io.Source
import scala.util.control.Breaks._

import java.io.File
import java.io._

import org.apache.spark.delta.DeltaWorkflowManager



class Test extends userTest[((Int, String), Int)] with Serializable {
	var num = 0
	def usrTest(inputRDD: RDD[((Int, String), Int)], test_num: Int, lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		val logger: Logger = Logger.getLogger(classOf[Test].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass (which returns false)
		var returnValue = false

		if (test_num == 0) {
			val finalRDD = inputRDD
			.map(pair => {
				(pair._1._1, pair._2)
			})
			.groupByKey
			.map(pair => {
				val itr = pair._2.toIterator
				var moving_average = 0.0
				var num = 1
				while (itr.hasNext) {
					moving_average = moving_average + (itr.next() - moving_average) / num
					//				moving_average = moving_average * (num - 1) / num + itr.next() / num

					num = num + 1
				}
				(pair._1, moving_average)
			})
			val start = System.nanoTime

			val out = finalRDD.collect()
			logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
			num = num + 1
			logger.log(Level.INFO, "TestRuns : " + num)
			println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

			breakable {
				for (o <- out) {
					if (o._1 > 3 || o._2 > 25 || o._2 < 18) {
						returnValue = true
						break
					}
				}
			}
		}
		if (test_num == 1) {
			val finalRDD = inputRDD
				.map(pair => {
					(pair._1._2, pair._2)
				})
				.groupByKey
				.map(pair => {
					val itr = pair._2.toIterator
					var moving_average = 0.0
					var num = 1
					while (itr.hasNext) {
						moving_average = moving_average + (itr.next() - moving_average) / num
						num = num + 1
					}
					(pair._1, moving_average)
				})
			val start = System.nanoTime

			val out = finalRDD.collect()
			logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
			num = num + 1
			logger.log(Level.INFO, "TestRuns : " + num)
			println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

			breakable {
				for (o <- out) {
					if (o._2 > 25 || o._2 < 18) {
						returnValue = true
						break
					}
				}
			}
		}
		returnValue
	}

	//FOR LOCAL COMPUTATION TEST WILL ALWAYS PASS
	def usrTest(inputRDD: Array[((Int, String), Int)], test_num: Int, lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		val logger: Logger = Logger.getLogger(classOf[Test].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass (which returns false)
		var returnValue = false

		if (test_num == 0) {
			val finalRDD = inputRDD
				.map(pair => {
					(pair._1._1, pair._2)
				})
				.groupBy(_._1)
				.map(pair => {
					val itr = pair._2.toIterator
					var moving_average = 0.0
					var num = 1
					while (itr.hasNext) {
						moving_average = moving_average + (itr.next()._2 - moving_average) / num
						//				moving_average = moving_average * (num - 1) / num + itr.next() / num

						num = num + 1
					}
					(pair._1, moving_average)
				})
			val start = System.nanoTime

			val out = finalRDD
			logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
			num = num + 1
			logger.log(Level.INFO, "TestRuns : " + num)
			println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

			breakable {
				for (o <- out) {
					if (o._1 > 3 || o._2 > 25 || o._2 < 18) {
						returnValue = true
						break
					}
				}
			}
		}
		if (test_num == 1) {
			val finalRDD = inputRDD
				.map(pair => {
					(pair._1._2, pair._2)
				})
				.groupBy(_._1)
				.map(pair => {
					val itr = pair._2.toIterator
					var moving_average = 0.0
					var num = 1
					while (itr.hasNext) {
						moving_average = moving_average + (itr.next()._2 - moving_average) / num
						num = num + 1
					}
					(pair._1, moving_average)
				})
			val start = System.nanoTime

			val out = finalRDD
			logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
			num = num + 1
			logger.log(Level.INFO, "TestRuns : " + num)
			println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

			breakable {
				for (o <- out) {
					if (o._2 > 25 || o._2 < 18) {
						returnValue = true
						break
					}
				}
			}
		}
		returnValue
	}

}

