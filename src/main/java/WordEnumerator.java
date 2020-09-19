import com.google.gson.Gson;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class Article {
    public String id;
    public String url;
    public String title;
    public String text;
}


public class WordEnumerator {
    public static class EnumeratorMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final Gson JSON_MAPPER = new Gson();

        private final static IntWritable one = new IntWritable(1);
        private final Text wordText = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] articlesRaw = value.toString().split("}\n");

            for (String raw : articlesRaw) {
                Article article = JSON_MAPPER.fromJson(raw + "}", Article.class);

                String[] words = article.text.split("\\s|\\.|,");

                for (String word : words) {
                    wordText.set(word);

                    context.write(wordText, one);
                }
            }
        }
    }

    public static class WordEnumeratorReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int id = Utils.hashOf(key.toString());

            context.write(new IntWritable(id), key);
        }
    }
}
