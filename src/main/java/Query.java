import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

public class Query {
    public static String KEY_QUERY_VECTOR = "KEY_QUERY_VECTOR";
    public static String KEY_QUERY_LIMIT = "KEY_QUERY_LIMIT";
    public static String OUTPUT_FILE_NAME = "part-r-00000";

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Should be at least 3 arguments - output path, query and output list length limit");
            System.exit(1);
            return;
        }

        String outputPath = args[0];

        int outputSize;

        try {
            outputSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Output size is not a number");

            System.exit(1);
            return;
        }

        String query = args[2];

        Configuration conf = new Configuration();

        runQueryVectorizer(conf, query);

        runRelevanceAnalyzer(conf, outputPath, outputSize);

        printResults(conf, outputPath);
    }

    private static void printResults(Configuration conf, String outputPath) throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);

        if (lastChar(outputPath) == '/') {
            outputPath = outputPath + OUTPUT_FILE_NAME;
        } else {
            outputPath = outputPath + '/' + OUTPUT_FILE_NAME;
        }

        InputStream is = fileSystem.open(new Path(outputPath));

        String data = IOUtils.toString(is, StandardCharsets.UTF_8);

        System.out.print(data);
    }

    private static void runQueryVectorizer(Configuration conf, String query) throws IOException {
        QueryVectorizer vectorizer = new QueryVectorizer(conf);

        Map<Long, Double> currentQueryVector = vectorizer.vectorize(query);

        String serializedQueryVector = Utils.serializeTfIdfMap(currentQueryVector);

        conf.set(KEY_QUERY_VECTOR, serializedQueryVector);
    }

    private static void runRelevanceAnalyzer(Configuration conf, String outputPath, int outputSize) throws Exception {
        conf.set(KEY_QUERY_LIMIT, String.valueOf(outputSize));

        Job job = Job.getInstance(conf, "query");

        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(RelevanceAnalizator.RelevanceMapper.class);
        job.setReducerClass(RelevanceAnalizator.RelevanceReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(DescendingDoubleComparator.class);

        FileInputFormat.addInputPath(job, new Path(Indexer.OUTPUT_DIR));

        Utils.deleteFolder(outputPath, conf);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);
    }

    private static char lastChar(String outputPath) {
        return outputPath.charAt(outputPath.length() - 1);
    }

    public static class DescendingDoubleComparator extends WritableComparator {

        protected DescendingDoubleComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            DoubleWritable k1 = (DoubleWritable) o1;
            DoubleWritable k2 = (DoubleWritable) o2;
            int cmp = k1.compareTo(k2);
            return -1 * cmp;
        }
    }
}
