import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryVectorizer {
    private final Map<Long, Integer> idfTable;

    public QueryVectorizer(Configuration configuration) throws IOException {
        this.idfTable = Utils.readIdfTable(configuration);
    }

    Map<Long, Double> vectorize(String query) {
        List<Word> words = Arrays.stream(query.split(" "))
                .map(Word::new)
                .collect(Collectors.toList());

        return Utils.vectorize(words, idfTable);
    }
}
