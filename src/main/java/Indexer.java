import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class Indexer {
    public static final String DOC_COUNTER_CACHE = "counter.temp";

    public static void main(String[] args) throws Exception {
        // Run counter

        runIndexer(args);
    }


    private static void runIndexer(String[] args) throws Exception {
        Configuration conf = new Configuration();
        File cache = new File(DOC_COUNTER_CACHE);

        Job job = Job.getInstance(conf, "indexer");
        job.setJarByClass(IndexerJob.class);
        job.setMapperClass(IndexerJob.IndexerMapper.class);
        job.setReducerClass(IndexerJob.IndexerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TFIDFWritable.class);
        job.addCacheFile(cache.toURI());

        String output = args[0];
        FileOutputFormat.setOutputPath(job, new Path(output));

        for (int i = 1; i < args.length; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        job.waitForCompletion(true);
    }
}
