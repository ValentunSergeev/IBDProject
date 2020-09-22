import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

public class Indexer {
    public static final String DOC_COUNTER_CACHE = "temp/part-r-00000";
    public static final String OUTPUT_DIR = "indexer_out/";

    public static void main(String[] args) throws Exception {
        runCounter(args);

        runIndexer(args);
    }

    private static void runCounter(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document count");
        job.setJarByClass(DocumentCount.class);
        job.setMapperClass(DocumentCount.DocCountMapper.class);
        job.setReducerClass(DocumentCount.DocCountReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        addInputFiles(args, job);

        Utils.deleteFolder("temp", conf);
        FileOutputFormat.setOutputPath(job, new Path("temp"));

        job.waitForCompletion(true);
    }

    private static void runIndexer(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "indexer");

        job.setJarByClass(IndexerJob.class);
        job.setMapperClass(IndexerJob.IndexerMapper.class);
        job.setReducerClass(IndexerJob.IndexerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Utils.deleteFolder(OUTPUT_DIR, conf);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

        addInputFiles(args, job);

        job.waitForCompletion(true);
    }

    private static void addInputFiles(String[] args, Job job) throws IOException {
        for (String arg : args) {
            FileInputFormat.addInputPath(job, new Path(arg));
        }
    }
}
