import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.collection.immutable.Seq

 
val configuration = new Configuration();
configuration.addResource(new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml"));
configuration.addResource(new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml"));

val inF = sc.textFile(FileSystem.get(configuration).getUri + "/ist597j/files/test1.txt");
val wc = inF.flatMap(line => line.split(' ')).map( word => (word,1)).cache()
wc.reduceByKey(_ + _).collect().foreach(println)
