import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class Word {
    public final String text;

    Word(String text) {
        this.text = text;
    }

    public long getId() {
        return Utils.hashOf(text);
    }
}

class Article {
    public String id;
    public String url;
    public String title;
    public String text;

    List<Word> getWords() {
        return Arrays.stream(text.split("\\s|\\.|,"))
                .map(Word::new)
                .collect(Collectors.toList());
    }
}
