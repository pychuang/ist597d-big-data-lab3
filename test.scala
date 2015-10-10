import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
 
val configuration = new Configuration();
configuration.addResource(new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml"));
configuration.addResource(new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml"));

val inF = sc.textFile(FileSystem.get(configuration).getUri + "/ist597j/files/test1.txt");
val words = inF.flatMap(line => line.split(' '))
val filteredWords = words.filter(w => w.length > 3)
val filteredWList = filteredWords.map(w => (w, 1))
filteredWList.reduceByKey(_+_).collect().foreach(println)
