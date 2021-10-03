import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.IOException




object ScalaMapReduce extends Configured with Tool {

  val IN_PATH_PARAM = "swap.input"
  val OUT_PATH_PARAM = "swap.output"

/*  override def run(args: Array[String]): Int = ???
  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }*/

  class SwapMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    val words = new Text()
    val amount = new IntWritable(1)
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val kv = value.toString.split("\\s")

      for (word <- kv) {
        words.set(word)
        context.write(words,amount)
      }
    }
  }

  class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    final private val result = new IntWritable

    @throws[IOException]
    @throws[InterruptedException]
    def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0

      for (i <- values) {
        sum += i.get
      }
      result.set(sum)
      context.write(key, result)
    }
  }

  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, "Word Count")
    job.setJarByClass(getClass)
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])
    job.setMapperClass(classOf[SwapMapper])
    job.setReducerClass(classOf[IntSumReducer])
    job.setNumReduceTasks(1)
    val in = new Path(getConf.get(IN_PATH_PARAM))
    val out = new Path(getConf.get(OUT_PATH_PARAM))
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)
    val fs = FileSystem.get(getConf)
    if (fs.exists(out)) fs.delete(out, true)
    if (job.waitForCompletion(true)) 0 else 1
  }

}