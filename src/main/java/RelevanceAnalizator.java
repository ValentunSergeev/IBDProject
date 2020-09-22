import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class RelevanceAnalizator {
    private static final String KEY_DELIMITER = "___";

    public static class RelevanceMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        private static final DoubleWritable relevanceWritable = new DoubleWritable();

        private Map<Long, Double> queryVector;

        @Override
        protected void setup(Context context) {
            String queryVectorRaw = context.getConfiguration().get(Query.KEY_QUERY_VECTOR);

            queryVector = Utils.deserializeTfIdfMap(queryVectorRaw);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyPair = value.toString().split("\t", 2);

            Map<Long, Double> tfidf = Utils.deserializeTfIdfMap(keyPair[1]);
            String articleCompoundIdentifier = keyPair[0];

            Set<Long> indexedWordSet = tfidf.keySet();
            Set<Long> queryWordSet = queryVector.keySet();

            indexedWordSet.retainAll(queryWordSet);

            double relevance = 0.0;

            for (Long wordId : indexedWordSet) {
                relevance += queryVector.get(wordId) * tfidf.get(wordId);
            }

            String articleName = Article.nameFromCompositeIdentifier(articleCompoundIdentifier);
            Text outputValue = new Text(articleName);

            relevanceWritable.set(relevance);

            context.write(relevanceWritable, outputValue);
        }
    }

    public static class RelevanceReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        private int queryLimit;

        @Override
        protected void setup(Context context) {
            queryLimit = Integer.parseInt(context.getConfiguration().get(Query.KEY_QUERY_LIMIT));
        }

        enum RelevanceCounter {
            TOP_PROCESSED
        }

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text article: values) {
                if (getProcessedCounter(context).getValue() > queryLimit) return;

                context.write(key, article);

                getProcessedCounter(context).increment(1);
            }
        }

        private Counter getProcessedCounter(Context context) {
            return context.getCounter(RelevanceCounter.TOP_PROCESSED);
        }
    }
}
