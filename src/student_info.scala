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



		//start recording lineage time
		val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
		val LineageStartTime = System.nanoTime()
		logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

		//spark program starts here
				lc.setCaptureLineage(true)


		val records = lc.textFile("../../Desktop/patientData.txt", 1)
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
//				moving_average = moving_average * (num - 1) / num + itr.next() / num

				num = num + 1
			}
			(pair._1, moving_average)
		})

		val out = average_age_by_grade.collectWithId()

		//print out the result for debugging purpose
		var list1 = List[Long]()
		for (o <- out) {
			if (o._1._1 > 3 || o._1._2 > 25 || o._1._2 < 18) {
				list1 = o._2 :: list1
			}
			println(o._1._1 + ": " + o._1._2 + " - " + o._2)
		}

		lc.setCaptureLineage(false)
		Thread.sleep(1000)

		var linRdd = average_age_by_grade.getLineage()
		linRdd.collect
		linRdd = linRdd.filter(s => {
			list1.contains(s)
//			s == 17179869185L
		})
		linRdd = linRdd.goBackAll()
		val show1Rdd = linRdd.show()

		println("Error inducing input from first error : " + show1Rdd.count())

		println(">>>>>>>>>>>>>>>>>>>>>>>>>>   First Lineage Tracing done  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<")


		lc.setCaptureLineage(true)

        //  val lc2 = new LineageContext(ctx)
        // lc2.setCaptureLineage(true)

        // Second run for overlapping lineage
        val major_age_pair = lc.textFile("../../Desktop/patientData.txt", 1).map(line => {
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
        var list2 = List[Long]()
        for (o <- out2) {
          if (o._1._2 > 25 || o._1._2 < 18) {
            list2 = o._2 :: list2
          }
          println(o._1._1 + ": " + o._1._2 + " - " + o._2)
        }


        // lc2.setCaptureLineage(false)
        //stop capturing lineage information
        lc.setCaptureLineage(false)
        Thread.sleep(1000)

        println(">>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

        var rdd2 = average_age_by_major.getLineage()
        rdd2.collect
        rdd2 = rdd2.filter(s => {
          list2.contains(s)
    //			s == 8589934592L

        })
        rdd2 = rdd2.goBackAll()
        val show2Rdd = rdd2.show()
        println("Error inducing input from second error : " + show2Rdd.count())



        val overlap = show1Rdd.intersection(show2Rdd)
    //		val array = overlap.map(s => s.asInstanceOf[(Int, Int)]._2)


        ///***To View Overlapping Data **/

    //		val mapped = rdd2.show().toRDD.map(s => (s.toString, 0L))

        println("Overlapping error inducing inputs from two lineages : " +
          overlap.count)

        val mappedRDD = overlap.map(s => {
          val list = s.split(" ")
          ((list(4).toInt, list(5)), list(3).toInt)
        })

        val delta_debug = new DD[((Int, String), Int)]
        var returnList = delta_debug.ddgen(mappedRDD, new Test, new Split_v2, lm, fh, List(false, false))

        println(">>>>>>>>>>>>>>>>>>>>>" + returnList)

        if (returnList(0) == false) {
          println("After DD again, running first test is needed")
          val mappedRdd1 = show1Rdd.map(s => {
            val list = s.split(" ")
            ((list(4).toInt, list(5)), list(3).toInt)
          })
          val delta_debug2 = new DD[((Int, String), Int)]
          returnList = delta_debug2.ddgen(mappedRdd1, new Test, new Split_v2, lm, fh, returnList)
        }
        if (returnList(1) == false) {
          println("After DD again, running second test is needed")
          val mappedRdd2 = show2Rdd.map(s => {
            val list = s.split(" ")
            ((list(4).toInt, list(5)), list(3).toInt)
          })
          val delta_debug3 = new DD[((Int, String), Int)]
          returnList = delta_debug3.ddgen(mappedRdd2, new Test, new Split_v2, lm, fh, returnList)
        }

        println("***************************" + returnList)

        val endTime = System.nanoTime()
        logger.log(Level.INFO, "Record total time: Delta-Debugging + Lineage + goNext:" + (endTime - LineageStartTime)/1000 + " microseconds")

		println("Job's DONE!")
		ctx.stop()

	}

}
