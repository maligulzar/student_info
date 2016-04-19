/**
 * Created by Michael on 4/14/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

//import java.util.List

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object student_info {
	private val exhaustive = 0

	def main(args: Array[String]): Unit = {
		//set up logging
		val lm: LogManager = LogManager.getLogManager
		val logger: Logger = Logger.getLogger(getClass.getName)
		val fh: FileHandler = new FileHandler("myLog")
		fh.setFormatter(new SimpleFormatter)
		lm.addLogger(logger)
		logger.setLevel(Level.INFO)
		logger.addHandler(fh)

		//set up spark configuration
		val sparkConf = new SparkConf().setMaster("local[6]")
		sparkConf.setAppName("Student_Info")
			.set("spark.executor.memory", "2g")

		//set up spark context
		val ctx = new SparkContext(sparkConf)


		//set up lineage context
		val lc = new LineageContext(ctx)
		lc.setCaptureLineage(true)

		//start recording lineage time
		val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
		val LineageStartTime = System.nanoTime()
		logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

		//spark program starts here
		val records = lc.textFile("src/patientData.txt", 1)
		// records.persist()
		val grade_age_pair = records.map(line => {
			val list = line.split(" ")
			(list(4).toInt, list(3).toInt)
		})
		val average_age_by_grade = grade_age_pair.groupByKey
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

		val out = average_age_by_grade.collectWithId()

		//print out the result for debugging purpose
		for (o <- out) {
			println(o._1._1 + ": " + o._1._2 + " - " + o._2)
		}

		lc.setCaptureLineage(false)
		Thread.sleep(1000)

		var linRdd = average_age_by_grade.getLineage()
		linRdd.collect
		linRdd = linRdd.filter(s => {
			//list.contains(s)
			s == 17179869185L
		})
		linRdd = linRdd.goBackAll()
		println("Error inducing input from first error : " + linRdd.count())

		// linRdd.collect().foreach(println)
		//   val show1Rdd = linRdd.show().toRDD

		println(">>>>>>>>>>>>>>>>>>>>>>>>>>   First Lineage Tracing done  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

		lc.setCaptureLineage(true)

		//  val lc2 = new LineageContext(ctx)
		// lc2.setCaptureLineage(true)

		// Second run for overlapping lineage
		val major_age_pair = lc.textFile("src/patientData.txt", 1).map(line => {
			val list = line.split(" ")
			(list(5), list(3).toInt)
		})
		val average_age_by_major = major_age_pair.groupByKey
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

		val out2 = average_age_by_major.collectWithId()

		//print out the result for debugging purpose
		for (o <- out2) {
			println(o._1._1 + ": " + o._1._2 + " - " + o._2)
		}


		// lc2.setCaptureLineage(false)
		//stop capturing lineage information
		lc.setCaptureLineage(false)
		Thread.sleep(1000)


		println(">>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

		//get the list of lineage id
		//      var list = List[Long]()
		//      var list2 = List[Long]()
		//      for (o <- out) {
		//        if ((o._1._1 == 0 && (o._1._2 > 19 || o._1._2 < 18))
		//                  || (o._1._1 == 1 && (o._1._2 > 21 || o._1._2 < 20))
		//                  || (o._1._1 == 2 && (o._1._2 > 23 || o._1._2 < 22))
		//                  || (o._1._1 == 3 && (o._1._2 > 25 || o._1._2 < 24))) {
		//          list = o._2 :: list
		//        }
		//      }

		var rdd2 = average_age_by_major.getLineage()
		rdd2.collect
		rdd2 = rdd2.filter(s => {
			//list.contains(s)
			s == 8589934592L
		})
		rdd2 = rdd2.goBackAll()
		println("Error inducing input from second error : " + rdd2.count())
		// val show2RDD = linRdd.show().toRDD
		val overlap = rdd2.intersection(linRdd).collect
		val array = overlap.map(s => s.asInstanceOf[(Int, Int)]._2)


		///***To View Overlapping Data **/
		//                     rdd2.filter {
		//                        s =>
		//                             array.contains(s.asInstanceOf[((Any,Int),Any)]._1._2)
		//                   }.show()
		               println("Overlapping error inducing inputs from two lineages : " +
		                   overlap.size)



		println("Job's DONE!")
		ctx.stop()

	}

}
