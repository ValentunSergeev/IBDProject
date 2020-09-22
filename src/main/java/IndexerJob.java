import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
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

                articleNameText.set(article.compoundIdentifier());

                for (Word word : words) {

                    wordText.set(word.text);

                    context.write(articleNameText, wordText);
                }
            }
        }
    }

    public static class IndexerReducer extends Reducer<Text, Text, Text, Text> {
        private Map<Long, Integer> idfMap;

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            idfMap = Utils.readIdfTable(context.getConfiguration());
        }

        @Override
        protected void reduce(Text articleIdentifier, Iterable<Text> wordsIterable, Context context) throws IOException, InterruptedException {
            List<Word> words = new ArrayList<>();

            wordsIterable.forEach(text -> words.add(new Word(text.toString())));

            Map<Long, Double> result = Utils.vectorize(words, idfMap);

            String resultJson = Utils.serializeTfIdfMap(result);

            Text resultWritable = new Text(resultJson);

            context.write(articleIdentifier, resultWritable);
        }

        @Override
        protected void cleanup(Context context) {
            idfMap = null;
        }
    }
}
