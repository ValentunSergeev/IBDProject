import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Map: returns (word, [1, 1, ...])
// Reduce: returns (word, how many docs contain this word)

public class DocumentCount {

    public static class DocCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private static final Text wordText = new Text();

        public void map(Object key, Text articleContent, Context context
        ) throws IOException, InterruptedException {
            List<Article> articles = Utils.readArticles(articleContent.toString());

            for(Article article : articles){
                List<Word> words = article.getWords();
                for (Word word : words) {
                    wordText.set(String.valueOf(word.getId()));
                    context.write(wordText, one);
                }
            }
        }
    }

    public static class DocCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> counts,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable cnt : counts) {
                sum += cnt.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document count");
        job.setJarByClass(DocumentCount.class);
        job.setMapperClass(DocCountMapper.class);
        job.setCombinerClass(DocCountReducer.class);
        job.setReducerClass(DocCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("counter.temp"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}