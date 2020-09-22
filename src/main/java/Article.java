import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class Word {
    public final String text;

    Word(String text) {
        this.text = text.toLowerCase();
    }

    public long getId() {
        return Utils.hashOf(text);
    }
}

class Article {
    private static final String COMPOUND_DELIMITER = ":::";

    public String id;
    public String url;
    public String title;
    public String text;

    String compoundIdentifier() {
        return id + COMPOUND_DELIMITER + title;
    }

    List<Word> getWords() {
        return Arrays.stream(text.split("\\s|\\.|,"))
                .map(Word::new)
                .collect(Collectors.toList());
    }

    public static String nameFromCompositeIdentifier(String compositeInentifier) {
        return compositeInentifier.split(COMPOUND_DELIMITER)[1];
    }
}
