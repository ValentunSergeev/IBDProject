import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexerJob {
    public static class IndexerMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text articleNameText = new Text();
        private static final Text wordText = new Text();

        @Override
        protected void map(Object key, Text articleContent, Context context) throws IOException, InterruptedException {
            List<Article> articles = Utils.readArticles(articleContent.toString());

            for (Article article : articles) {
                List<Word> words = article.getWords();

                articleNameText.set(article.id);

                for (Word word : words) {

                    wordText.set(word.text);

                    context.write(articleNameText, wordText);
                }
            }
        }
    }

    public static class IndexerReducer extends Reducer<Text, Text, Text, TFIDFWritable> {
        private Map<Long, Integer> idfMap;

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            InputStream is = fs.open(new Path("temp/part-r-00000"));

            BufferedReader reader = new BufferedReader(new InputStreamReader(is));

            idfMap = Utils.readIdfTable(reader);
        }

        @Override
        protected void reduce(Text articleName, Iterable<Text> wordsIterable, Context context) throws IOException, InterruptedException {
            List<Word> words = new ArrayList<>();

            wordsIterable.forEach(text -> words.add(new Word(text.toString())));

            List<Pair<Long, Double>> result = Utils.vectorize(words, idfMap);

            TFIDFWritable resultWritable = new TFIDFWritable(result);

            context.write(articleName, resultWritable);
        }

        @Override
        protected void cleanup(Context context) {
            idfMap = null;
        }
    }
}
