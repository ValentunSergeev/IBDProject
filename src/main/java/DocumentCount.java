import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
            extends Mapper<Object, Text, LongWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private static final LongWritable wordID = new LongWritable();

        public void map(Object key, Text articleContent, Context context
        ) throws IOException, InterruptedException {
            List<Article> articles = Utils.readArticles(articleContent.toString());

            for (Article article : articles) {
                List<Word> words = article.getWords();
                Set<String> contents = words.stream().map(word -> word.text).collect(Collectors.toSet());

                for (String word : contents) {
                    long id = Utils.hashOf(word);
                    wordID.set(id);
                    context.write(wordID, one);
                }
            }
        }
    }

    public static class DocCountReducer
            extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(LongWritable key, Iterable<IntWritable> counts,
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
}