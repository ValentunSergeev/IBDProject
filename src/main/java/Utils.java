import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.lang.reflect.Type;
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
        String lower = string.toLowerCase();

        long hash = 7;
        for (int i = 0; i < lower.length(); i++) {
            hash = hash * 31 + lower.charAt(i);
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

    public static Map<Long, Integer> readIdfTable(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        InputStream is = fs.open(new Path(Indexer.DOC_COUNTER_CACHE));

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        Map<Long, Integer> idfMap = new HashMap<>();

        String line;

        while ((line = reader.readLine()) != null) {
            String[] keypair = line.split("\t");

            String rawId = keypair[0];
            int idf = Integer.parseInt(keypair[1]);

            idfMap.put(Long.parseLong(rawId), idf);
        }

        return idfMap;
    }

    public static Map<Long, Double> vectorize(List<Word> words, Map<Long, Integer> idfMap) {
        Map<Long, Integer> frequencyMap = new HashMap<>();

        for (Word word : words) {
            Integer newValue = frequencyMap.getOrDefault(word.getId(), 0) + 1;
            frequencyMap.put(word.getId(), newValue);
        }

        Map<Long, Double> result = new HashMap<>(frequencyMap.size());

        for (Map.Entry<Long, Integer> entry : frequencyMap.entrySet()) {
            long id = entry.getKey();
            Integer value = entry.getValue();
            double valueDouble = value.doubleValue();
            Integer norm = idfMap.get(id);

            if (norm != null) {
                Double normalized = valueDouble / norm;
                result.put(id, normalized);
            }
        }

        return result;
    }

    public static String serializeTfIdfMap(Map<Long, Double> map) {
        return JSON_MAPPER.toJson(map);
    }

    public static Map<Long, Double> deserializeTfIdfMap(String source) {
        Type typeOfHashMap = new TypeToken<Map<Long, Double>>() { }.getType();

        return JSON_MAPPER.fromJson(source, typeOfHashMap);
    }

    public static void deleteFolder(String folder, Configuration configuration) throws IOException {
        Path path = new Path(folder);
        FileSystem fileSystem = FileSystem.get(configuration);

        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }
}
