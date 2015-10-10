import java.io.File
import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.collection.immutable.Seq

val configuration = new Configuration()
configuration.addResource(new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml"))
configuration.addResource(new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml"))

// I got "SparkException: Task not serializable" if I use function for map()
// so I just use anonymous functions instead of functions

val lines = sc.textFile(FileSystem.get(configuration).getUri + "/ist597j/tweets/nyc-twitter-data-2013.csv")
val texts = lines.map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")).map(cs => (if (cs.size < 7) ""; else cs(6)))
val words = texts.flatMap(_.split("[ .,?!:\"]"))
val hashtags = words.filter(x => x.size > 0 && x(0) == '#')
val htStats = hashtags.map(h => (h, 1)).reduceByKey(_+_)
val top100 = htStats.sortBy(_._2, false).take(100)

// output
val outputFile = "output.txt"
val writer = new PrintWriter(new File(outputFile))
top100.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
writer.close()
