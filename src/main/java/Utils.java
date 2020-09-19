import com.google.gson.Gson;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    private static final Gson JSON_MAPPER = new Gson();

    /**
     * Simple polynomial hash function for strings
     */
    public static int hashOf(String string) {
        int hash = 7;
        for (int i = 0; i < string.length(); i++) {
            hash = hash * 31 + string.charAt(i);
        }

        return hash;
    }

    public static List<Article> readArticles(String source) {
        String[] articlesRaw = source.toString().split("}\n");

        List<Article> result = new ArrayList<>(articlesRaw.length);

        for (String raw : articlesRaw) {
            Article article = JSON_MAPPER.fromJson(raw + "}", Article.class);

            result.add(article);
        }

        return result;
    }

    public static Map<Integer, Integer> readIdfTable(URI source) throws IOException {
        Map<Integer, Integer> idfMap = new HashMap<>();

        File file = new File(source);

        Files.lines(file.toPath())
                .forEach(line -> {
                    String[] keypair = line.split(":");

                    String text = keypair[0];
                    int idf = Integer.parseInt(keypair[1]);

                    idfMap.put(hashOf(text), idf);
                });

        return idfMap;
    }

    public static List<Pair<Integer, Double>> vectorize(List<Word> words, Map<Integer, Integer> idfMap) {
        Map<Integer, Integer> frequencyMap = new HashMap<>();

        for (Word word: words) {
            var newValue = frequencyMap.getOrDefault(word.getId(), 0) + 1;
            frequencyMap.put(word.getId(), newValue);
        }

        var result = new ArrayList<Pair<Integer, Double>>(frequencyMap.size());

        for (Map.Entry<Integer, Integer> entry: frequencyMap.entrySet()) {
            var id = entry.getKey();
            var normalized = entry.getValue().doubleValue() / idfMap.get(id);

            result.add(new Pair<>(id, normalized));
        }

        return result;
    }
}
