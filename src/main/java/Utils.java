import com.google.gson.Gson;
import org.apache.commons.math3.util.Pair;

import java.io.BufferedReader;
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
    public static long hashOf(String string) {
        long hash = 7;
        for (int i = 0; i < string.length(); i++) {
            hash = hash * 31 + string.charAt(i);
        }

        return hash;
    }

    public static List<Article> readArticles(String source) {
        String[] articlesRaw = source.split("}\n");

        List<Article> result = new ArrayList<>(articlesRaw.length);

        for (String raw : articlesRaw) {
            Article article = JSON_MAPPER.fromJson(raw, Article.class);

            result.add(article);
        }

        return result;
    }

    public static Map<Long, Integer> readIdfTable(BufferedReader source) throws IOException {
        Map<Long, Integer> idfMap = new HashMap<>();

        String line;

        while ((line = source.readLine()) != null) {
            String[] keypair = line.split("\t");

            String rawId = keypair[0];
            int idf = Integer.parseInt(keypair[1]);

            idfMap.put(Long.parseLong(rawId), idf);
        }

        return idfMap;
    }

    public static List<Pair<Long, Double>> vectorize(List<Word> words, Map<Long, Integer> idfMap) {
        Map<Long, Integer> frequencyMap = new HashMap<>();

        for (Word word : words) {
            Integer newValue = frequencyMap.getOrDefault(word.getId(), 0) + 1;
            frequencyMap.put(word.getId(), newValue);
        }

        ArrayList<Pair<Long, Double>> result = new ArrayList<>(frequencyMap.size());

        for (Map.Entry<Long, Integer> entry : frequencyMap.entrySet()) {
            long id = entry.getKey();
            Integer value = entry.getValue();
            double valueDouble = value.doubleValue();
            Integer norm = idfMap.get(id);

            Double normalized = valueDouble / norm;

            result.add(new Pair<>(id, normalized));
        }

        return result;
    }
}
