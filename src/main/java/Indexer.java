import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Indexer {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        runEnumerator(args);
    }

    private static void runEnumerator(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word enumerator");
        job.setJarByClass(WordEnumerator.class);
        job.setMapperClass(WordEnumerator.EnumeratorMapper.class);
        job.setReducerClass(WordEnumerator.WordEnumeratorReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        String output = args[0];
        FileOutputFormat.setOutputPath(job, new Path(output));

        for (int i = 1; i < args.length; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        job.waitForCompletion(true);
    }
}
