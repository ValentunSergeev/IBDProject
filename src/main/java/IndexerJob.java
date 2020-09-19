import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.IOException;
import java.net.URI;
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
        private Map<Integer, Integer> idfMap;

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            URI[] uriList = DistributedCache.getCacheFiles(context.getConfiguration());

            idfMap = Utils.readIdfTable(uriList[0]);
        }

        @Override
        protected void reduce(Text articleName, Iterable<Text> wordsIterable, Context context) throws IOException, InterruptedException {
            var words = new ArrayList<Word>();

            wordsIterable.forEach(text -> words.add(new Word(text.toString())));

            var result = Utils.vectorize(words, idfMap);

            var resultWritable = new TFIDFWritable(result);

            context.write(articleName, resultWritable);
        }

        @Override
        protected void cleanup(Context context) {
            idfMap = null;
        }
    }
}
