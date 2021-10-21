package MapReduce

import java.lang.Iterable
import java.util.StringTokenizer

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters._
import scala.util.matching.Regex

package object WordCountAnalytics {

  class TypeCounter extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val text = new StringTokenizer(value.toString)
      while (text.hasMoreTokens()) {
        val str = text.nextToken()
        if(str == config.getString("wordCount.logType.error") || str == config.getString("wordCount.logType.info") || str == config.getString("wordCount.logType.warn") || str == config.getString("wordCount.logType.debug")) {
          word.set(str)
          context.write(word, one)
        }
      }
    }
  }

  class DistributionCal extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val one = new IntWritable(1)
    val message = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val text = value.toString.split("\n")
      System.out.println(text)
      var str = ""
      for(line <- text){
        val words = line.split(" ")
        for(word <- words){
          if(word == config.getString("wordCount.logType.error") || word == config.getString("wordCount.logType.info") || word == config.getString("wordCount.logType.warn") || word == config.getString("wordCount.logType.debug")){
            str = str + words(0) + " " + word + " " + words(words.length - 1) + "\n"
          }
        }
        if((text.indexOf(line) + 1) % 10 == 0){
          System.out.println(str)
          message.set(str)
          context.write(message, one)
          str = "\n\n\n"
        }
      }
    }
  }

  class ErrorDistribution extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val one = new IntWritable(1)
    val message = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val text = value.toString.split(("\n"))
      var str = ""
      for(line <- text){
        val words = line.split(" ")
        for(word <- words){
          if(word == config.getString("wordCount.logType.error")){
            str = str + words(0) + " " + word + " " + words(words.length - 1) + "\n"
          }
        }
        if((text.indexOf(line) + 1) % 10 == 0){
          System.out.println(str)
          message.set(str)
          context.write(message, one)
          str = "\n\n\n"
        }
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class LogReaderWithTS extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class ErrorDistributionSorter extends Reducer[Text,IntWritable,Text,IntWritable] {

    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }


  def main(args: Array[String]): Unit = {

    // Config setup for the first mapreduce job
    val configuration1 = new Configuration
    configuration1.set("mapred.textoutputformat.separator", ",")
    val job1 = Job.getInstance(configuration1,"Log Type Count")
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[TypeCounter])
    job1.setCombinerClass(classOf[IntSumReader])
    job1.setReducerClass(classOf[IntSumReader])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputKeyClass(classOf[Text]);
    job1.setOutputValueClass(classOf[IntWritable]);
    job1.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1)))
    job1.waitForCompletion(true)

    // Config setup for the second mapreduce job
    val configuration2 = new Configuration
    configuration2.set("mapred.textoutputformat.separator", ",")
    val job2 = Job.getInstance(configuration2,"Time Interval Distribution")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[DistributionCal])
    job2.setCombinerClass(classOf[LogReaderWithTS])
    job2.setReducerClass(classOf[LogReaderWithTS])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text]);
    job2.setOutputValueClass(classOf[IntWritable]);
    job2.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job2, new Path(args(0)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))
    job2.waitForCompletion(true)

    // Config setup for the third mapreduce job
    val configuration3 = new Configuration
    configuration3.set("mapred.textoutputformat.separator", ",")
    val job3 = Job.getInstance(configuration2,"Max Error logs")
    job3.setJarByClass(this.getClass)
    job3.setMapperClass(classOf[ErrorDistribution])
    job3.setCombinerClass(classOf[ErrorDistributionSorter])
    job3.setReducerClass(classOf[ErrorDistributionSorter])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputKeyClass(classOf[Text]);
    job3.setOutputValueClass(classOf[IntWritable]);
    job3.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job3, new Path(args(0)))
    FileOutputFormat.setOutputPath(job3, new Path(args(3)))
    job3.waitForCompletion(true)

    System.exit(if(job3.waitForCompletion(true))  0 else 1)
  }
}