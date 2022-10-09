import java.lang.Iterable
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

object ScalaMapReduce extends Configured with Tool {
  val IN_PATH_PARAM = "wordcount.input"
  val OUT_PATH_PARAM = "wordcount.output"

  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }

  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, "Word Count")
    job.setJarByClass(getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setNumReduceTasks(2)
    val in = new Path(getConf.get(IN_PATH_PARAM))
    val out = new Path(getConf.get(OUT_PATH_PARAM))
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)
    if (job.waitForCompletion(true)) 0 else 1
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    val text = new Text()

    override def map(key: Object, value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val words = value.toString.trim().replaceAll("\\pP", "").toLowerCase().split("\\s+")
      for (word <- words) {
        text.set(word)
        context.write(text, one)
      }
    }
  }

  class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    val result = new IntWritable()

    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      values.forEach(value => sum += value.get())
      result.set(sum)
      context.write(key, result)
    }
  }
}