/**
 * Created by Michael on 4/14/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.{SparkConf, SparkContext}
//import java.util.List

//remove if not needed
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object student_info {
  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
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
      val records = lc.textFile("patientData.txt", 1)
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
          moving_average = moving_average + (itr.next() - moving_average)/num
          num = num + 1
        }
        (pair._1, moving_average)
      })

      val out = average_age_by_grade.collectWithId()

//      val major_age_pair = records.map(line => {
//        val list = line.split(" ")
//        (list(5), list(3).toInt)
//      })
//      val average_age_by_major = major_age_pair.groupByKey
//      .map(pair => {
//        val itr = pair._2.toIterator
//        var moving_average = 0.0
//        var num = 1
//        while (itr.hasNext) {
//          moving_average = moving_average + (itr.next() - moving_average)/num
//          num = num + 1
//        }
//        (pair._1, moving_average)
//      })
//
//      val out2 = average_age_by_major.collectWithId()


      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purpose
      for (o <- out) {
        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      }

      println(">>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

//      for (o <- out2) {
//        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
//      }


      //      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/WordCount_CLDD/lineageResult"))

      //get the list of lineage id
      var list = List[Long]()
      var list2 = List[Long]()
      for (o <- out) {
        if ((o._1._1 == 0 && (o._1._2 > 19 || o._1._2 < 18))
                  || (o._1._1 == 1 && (o._1._2 > 21 || o._1._2 < 20))
                  || (o._1._1 == 2 && (o._1._2 > 23 || o._1._2 < 22))
                  || (o._1._1 == 3 && (o._1._2 > 25 || o._1._2 < 24))) {
          list = o._2 :: list
        }
      }
//      for (o <- out2) {
//        if (o._1._2 > 25) {
//          list2 = o._2 :: list2
//        }
//      }

      var linRdd = average_age_by_grade.getLineage()
      linRdd.collect
      linRdd  = linRdd.filter(s => {
        list.contains(s)
      })
      linRdd = linRdd.goBackAll()
      val showMeRdd = linRdd.show().toRDD
//
//      println(">>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
//
//      linRdd = average_age_by_grade.getLineage()
//      linRdd.collect
//      linRdd  = linRdd.filter(s => list2.contains(s))
//      linRdd = linRdd.goBackAll()
//      val showMeRdd2 = linRdd.show().toRDD

      //At this stage, technically lineage has already find all the faulty data set, we record the time
//      val lineageEndTime = System.nanoTime()
//      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
//      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)
//      val showMeRdd2 = linRdd.show().toRDD
/*

            val mappedRDD = showMeRdd.map(s => {
              val str = s.toString
              val index = str.lastIndexOf(",")
              val lineageID = str.substring(index + 1, str.length - 1)
              val content = str.substring(2, index - 1)
              val index2 = content.lastIndexOf(",")
              ((content.substring(0, index2), content.substring(index2 + 1).toInt), lineageID.toLong)
            })

            mappedRDD.cache()
            //      println("MappedRDD has " + mappedRDD.count() + " records")



            //      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/WordCount_CLDD/lineageResult", 1)
            //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/textFile", 1)

            //      val num = lineageResult.count()
            //      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


            //Remove output before delta-debugging
            val outputFile = new File("/Users/Michael/IdeaProjects/WordCount_CLDD/output")
            if (outputFile.isDirectory) {
              for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
            }
            outputFile.delete

            val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
            val DeltaDebuggingStartTime = System.nanoTime()
            logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

            /** **************
              * **********
              */
            //lineageResult.cache()



            //      if (exhaustive == 1) {
            //        val delta_debug = new DD[(String, Int)]
            //        delta_debug.ddgen(mappedRDD, new Test,
            //          new Split, lm, fh)
            //      } else {
            val delta_debug = new DD_NonEx_NonIncr[(String, Int), Long]
            val returnedRDD = delta_debug.ddgen(mappedRDD, new Test_NonIncr, new Split, lm, fh)
            //      }
            val ss = returnedRDD.collect
            //      println("**************")
            //      for (a <- ss){
            //        println(a._1 + " && " + a._2)
            //      }
            //      println("**************")
            // linRdd.collect.foreach(println)
            linRdd = wordCount.getLineage()
            linRdd.collect
            linRdd = linRdd.goBack().goBack().filter(l => {
              if(l.asInstanceOf[((Int, Int),(Int, Int))]._1._2 == ss(0)._2.toInt){
                println("*** => " + l)
                true
              }else false
            })
            linRdd = linRdd.goBackAll()
            linRdd.collect()
            linRdd.show()

            val DeltaDebuggingEndTime = System.nanoTime()
            val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
            logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
            logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")

            //total time
            logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (DeltaDebuggingEndTime - LineageStartTime)/1000 + " microseconds")
      */
      println("Job's DONE!")
      ctx.stop()
    }
  }

}
