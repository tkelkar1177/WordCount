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
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.WritableComparator
import math.Ordering.Implicits.infixOrderingOps

package object WordCountAnalytics {

  /* Mappers and Reducers for the first task. The logic involves creating groups based on the timestamp converted to seconds
     The first mapper and reducer created the groups and adds up the total number of each log level per group. The second
     mapper and reducer is tasked with linking the timestamp for each log to the log level and the sum of each level in
     the corresponding group*/

  class Task1Mapper1 extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val text = value.toString.split("\n")
      for(line <- text){
        val words = line.split(" ")
        for(word <- words){
          if(word == config.getString("wordCount.logType.error") || word == config.getString("wordCount.logType.info") || word == config.getString("wordCount.logType.warn") || word == config.getString("wordCount.logType.debug")){
            val intervalGroup = (Integer.parseInt(words(0).substring(0,2)) * 3600 + Integer.parseInt(words(0).substring(3,5)) * 60 + Integer.parseInt(words(0).substring(6,8))) / config.getInt("wordCount.interval")
            val str = intervalGroup.toString + " " + word
            context.write(new Text(str), one)
          }
        }
      }
    }
  }

  class Task1Reducer1 extends Reducer[Text,IntWritable,Text,IntWritable] {

    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class Task1Mapper2 extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val newKey = value.toString.split(" ")
      val sum = newKey(1).toString.split(",")
      val timestamp = (Integer.parseInt(newKey(0).toString) * config.getInt("wordCount.interval") / 3600).toInt.toString + ":" + ((Integer.parseInt(newKey(0).toString) * config.getInt("wordCount.interval") / 60).toInt % 60).toString + ":" + (Integer.parseInt(newKey(0).toString) * config.getInt("wordCount.interval") % 60).toString
      context.write(new Text(timestamp + "  " + sum(0).toString), new IntWritable(sum(1).toString.toInt))
    }
  }

  class Task1Reducer2 extends Reducer[Text,IntWritable,Text,IntWritable] {

    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      values.forEach(text => {context.write(key, text)})
    }
  }

  class DescendingKeyComparator extends WritableComparator(classOf[IntWritable], true){
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
      val val1 = a.asInstanceOf[IntWritable]
      val val2 = b.asInstanceOf[IntWritable]
      -1 * val1.compareTo(val2)
    }
  }

  /* Mappers and Reducers for the second task. The logic is the similar to Task 2 with a difference being that we only
     look for the log level ERROR and ordered in the descending order of the occurrence of the log level in each
     timestamp*/

  class Task2Mapper1 extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val text = value.toString.split("\n")
      for(line <- text){
        val words = line.split(" ")
        for(word <- words){
          if(word == config.getString("wordCount.logType.error")){
            val intervalGroup = (Integer.parseInt(words(0).substring(0,2)) * 3600 + Integer.parseInt(words(0).substring(3,5)) * 60 + Integer.parseInt(words(0).substring(6,8))) / config.getInt("wordCount.interval")
            val str = intervalGroup.toString + " " + word
            context.write(new Text(str), one)
          }
        }
      }
    }
  }

  class Task2Reducer1 extends Reducer[Text,IntWritable,Text,IntWritable] {

    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class Task2Mapper2 extends Mapper[Object, Text, IntWritable, Text] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val newKey = value.toString.split(" ")
      val sum = newKey(1).toString.split(",")
      val timestamp = (Integer.parseInt(newKey(0).toString) * config.getInt("wordCount.interval") / 3600).toInt.toString + ":" + ((Integer.parseInt(newKey(0).toString) * config.getInt("wordCount.interval") / 60).toInt % 60).toString + ":" + (Integer.parseInt(newKey(0).toString) * config.getInt("wordCount.interval") % 60).toString
      context.write(new IntWritable(sum(1).toString.toInt), new Text(timestamp + "  " + sum(0).toString))
    }
  }

  class Task2Reducer2 extends Reducer[IntWritable,Text,IntWritable,Text] {

    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, IntWritable, Text]#Context): Unit = {
      values.forEach(text => {context.write(key, text)})
    }
  }

  /* Mapper and Reducer for the third task. The logic involves comparing each space separated string in the input text
     file with each log level. if theres a match, we map that level with the iterable integer value 1. The reducer
     takes such key values corresponding with each log level and a "1" which is summed up and output in a file as the
     final output*/

  class Task3Mapper extends Mapper[Object, Text, Text, IntWritable] {

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

  class Task3Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  /* Mappers and Reducers for the fourth task. The logic involves a similar logic to Task 3 with the difference arising
     the fact that in the reducer, we find the max count of each log level in a timestamp compared with all other
     timestamps*/

  class Task4Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val config = ObtainConfigReference("wordCount") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val text = value.toString.split("\n")
      for(line <- text){
        val words = line.split(" ")
        for(word <- words){
          if(word == config.getString("wordCount.logType.error") || word == config.getString("wordCount.logType.info") || word == config.getString("wordCount.logType.warn") || word == config.getString("wordCount.logType.debug")){
            context.write(new Text(word), new IntWritable(words(words.length - 1).length))
          }
        }
      }
    }
  }

  class Task4Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val max = values.asScala.foldLeft(0)((a, b) => math.max(a.toString.toInt,b.toString.toInt))
      context.write(key, new IntWritable(max))
    }
  }

  def main(args: Array[String]): Unit = {

    val logger = CreateLogger(classOf[main])

    logger.info(" Starting Job 1 for Task 1 :")
    val task1configuration1 = new Configuration
    task1configuration1.set("mapred.textoutputformat.separator", ",")
    val task1job1 = Job.getInstance(task1configuration1,"Time Interval Distribution")
    task1job1.setJarByClass(this.getClass)
    task1job1.setMapperClass(classOf[Task1Mapper1])
    task1job1.setCombinerClass(classOf[Task1Reducer1])
    task1job1.setReducerClass(classOf[Task1Reducer1])
    task1job1.setOutputKeyClass(classOf[Text])
    task1job1.setOutputKeyClass(classOf[Text]);
    task1job1.setOutputValueClass(classOf[IntWritable]);
    task1job1.setNumReduceTasks(1)
    FileInputFormat.addInputPath(task1job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(task1job1, new Path(args(1)))
    task1job1.waitForCompletion(true)
    logger.info(" Job 1 for Task 1 has ended")

    logger.info(" Starting Job 2 for Task 2 :")
    val task1configuration2 = new Configuration
    task1configuration2.set("mapred.textoutputformat.separator", ",")
    val task1job2 = Job.getInstance(task1configuration2,"Time Interval Distribution")
    task1job2.setJarByClass(this.getClass)
    task1job2.setMapperClass(classOf[Task1Mapper2])
    task1job2.setCombinerClass(classOf[Task1Reducer2])
    task1job2.setReducerClass(classOf[Task1Reducer2])
    task1job2.setOutputKeyClass(classOf[Text])
    task1job2.setOutputKeyClass(classOf[Text]);
    task1job2.setOutputValueClass(classOf[IntWritable]);
    task1job2.setNumReduceTasks(1)
    FileInputFormat.addInputPath(task1job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(task1job2, new Path(args(2)))
    task1job2.waitForCompletion(true)
    logger.info(" Job 2 for Task 1 has ended")

    logger.info(" Starting Job 1 for Task 2 :")
    val task2configuration1 = new Configuration
    task2configuration1.set("mapred.textoutputformat.separator", ",")
    val task2job1 = Job.getInstance(task2configuration1,"Time Interval Distribution")
    task2job1.setJarByClass(this.getClass)
    task2job1.setMapperClass(classOf[Task2Mapper1])
    task2job1.setCombinerClass(classOf[Task2Reducer1])
    task2job1.setReducerClass(classOf[Task2Reducer1])
    task2job1.setOutputKeyClass(classOf[Text])
    task2job1.setOutputKeyClass(classOf[Text]);
    task2job1.setOutputValueClass(classOf[IntWritable]);
    task2job1.setNumReduceTasks(1)
    FileInputFormat.addInputPath(task2job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(task2job1, new Path(args(3)))
    task2job1.waitForCompletion(true)
    logger.info(" Job 1 for Task 2 has ended")

    logger.info(" Starting Job 2 for Task 2 :")
    val task2configuration2 = new Configuration
    task2configuration2.set("mapred.textoutputformat.separator", ",")
    val task2job2 = Job.getInstance(task2configuration2,"Time Interval Distribution")
    task2job2.setJarByClass(this.getClass)
    task2job2.setMapperClass(classOf[Task2Mapper2])
    task2job2.setReducerClass(classOf[Task2Reducer2])
    task2job2.setSortComparatorClass(classOf[DescendingKeyComparator])
    task2job2.setOutputKeyClass(classOf[Text])
    task2job2.setOutputKeyClass(classOf[IntWritable]);
    task2job2.setOutputValueClass(classOf[Text]);
    task2job2.setNumReduceTasks(1)
    FileInputFormat.addInputPath(task2job2, new Path(args(3)))
    FileOutputFormat.setOutputPath(task2job2, new Path(args(4)))
    task2job2.waitForCompletion(true)
    logger.info(" Job 2 for Task 2 has ended")

    logger.info(" Starting Job for Task 3 :")
    val task3configuration = new Configuration
    task3configuration.set("mapred.textoutputformat.separator", ",")
    val task3job = Job.getInstance(task3configuration,"Time Interval Distribution")
    task3job.setJarByClass(this.getClass)
    task3job.setMapperClass(classOf[Task3Mapper])
    task3job.setCombinerClass(classOf[Task3Reducer])
    task3job.setReducerClass(classOf[Task3Reducer])
    task3job.setOutputKeyClass(classOf[Text])
    task3job.setOutputKeyClass(classOf[Text]);
    task3job.setOutputValueClass(classOf[IntWritable]);
    task3job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(task3job, new Path(args(0)))
    FileOutputFormat.setOutputPath(task3job, new Path(args(5)))
    task3job.waitForCompletion(true)
    logger.info(" Job for Task 3 has ended")

    logger.info(" Starting Job for Task 4 :")
    val task4configuration = new Configuration
    task4configuration.set("mapred.textoutputformat.separator", ",")
    val task4job = Job.getInstance(task4configuration,"Time Interval Distribution")
    task4job.setJarByClass(this.getClass)
    task4job.setMapperClass(classOf[Task4Mapper])
    task4job.setCombinerClass(classOf[Task4Reducer])
    task4job.setReducerClass(classOf[Task4Reducer])
    task4job.setOutputKeyClass(classOf[Text])
    task4job.setOutputKeyClass(classOf[Text]);
    task4job.setOutputValueClass(classOf[IntWritable]);
    task4job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(task4job, new Path(args(0)))
    FileOutputFormat.setOutputPath(task4job, new Path(args(6)))
    logger.info(" Job for Task 4 has ended")

    logger.info(" All tasks have ended. Exiting")
    System.exit(if(task4job.waitForCompletion(true))  0 else 1)
  }
}