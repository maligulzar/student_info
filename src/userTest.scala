/**
 * Created by Michael on 11/12/15.
 */

import java.util.logging.{FileHandler, LogManager}

import org.apache.spark.rdd.RDD

//remove if not needed

trait userTest[T] {

  def usrTest(inputRDD: RDD[T], lm: LogManager, fh: FileHandler): Boolean
}
