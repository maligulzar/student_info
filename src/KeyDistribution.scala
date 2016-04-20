/**
 * Created by ali on 4/20/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

//import java.util.List

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object KeyDistribution {
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
			if(o._1._2.asInstanceOf[Float] > 25){

			}
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
		println("Job's DONE!")
		ctx.stop()

	}

}
