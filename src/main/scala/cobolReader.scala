/**
 *
 *
 * @author <a href="mailto:<pedro.bastos@xpand-it.com>"><Pedro Bastos></a>
 * @version \$Revision: 666 $
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
object cobolReader {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("main").setMaster("local") //.set("spark.driver.extraClassPath", "src/main/resources/core-site.xml")
    val spark = SparkSession
      .builder()
      .config(conf = sparkConf)
      .config("dfs.client.read.shortcircuit.skip.checksum", "true")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.setVerifyChecksum(false)

    val cobolDataframe = spark.read
      .format("za.co.absa.cobrix.spark.cobol.source")
      .option("copybook", "src/main/resources/UKAJRNL.cbk.txt")
      .option("schema_retention_policy", "collapse_root")
      .load("src/main/resources/C.PGMLNGL.FKM001.041212.20201123")

    cobolDataframe
      .take(1)
      .foreach(v => println(v))
    val output = cobolDataframe.coalesce(1).write.mode(SaveMode.Overwrite).format("avro").save("C.PGMLNGL.FKM001.041212.20201123.avro")
  }
}
