import java.util.List;

public class Article {
    public String uuid;
    public String title;
    public String author;
    public String url;
    public String text;
    public String published;
    public String language;
    public List<String> categories;

    public Article(){}

    public Article(String uuid, String title, String author, String url, String text,
                   String published, String language, List<String> categories) {
        this.uuid = uuid;
        this.title = title;
        this.author = author;
        this.url = url;
        this.text = text;
        this.published = published;
        this.language = language;
        this.categories = categories;
    }
}
