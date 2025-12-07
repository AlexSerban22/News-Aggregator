import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;



public class MyThread implements Runnable {
    public int threadId;

    public MyThread(int threadId) {
        this.threadId = threadId;
    }

    @Override
    public void run() {
        processFiles();
        try {
            Tema1.barrier.await();
        } catch (Exception ignored) {}

        writeCategories();
        writeLanguages();
    }

    private void processFiles() {
        while (true) {
            String filePath = Tema1.articlesQueue.poll();
            if (filePath == null) {
                break;
            }
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode rootNode = mapper.readTree(new File(filePath));
                if (rootNode.isArray()) {
                    for (JsonNode articleNode : rootNode) {
                        Article article = parseArticle(articleNode);
                        // adăugăm articolul în structurile de date comune
                        if (Tema1.articlesByLanguage.containsKey(article.language))
                            Tema1.articlesByLanguage.get(article.language).add(article.uuid);
                        for (String category : article.categories) {
                            if (Tema1.articlesByCategory.containsKey(category)) {
                                Tema1.articlesByCategory.get(category).add(article.uuid);
                            }
                        }

                        if (Tema1.articlesByTitle.containsKey(article.title)) {
                            String dupedUuid = Tema1.articlesByTitle.get(article.title).uuid;
                            Tema1.articlesByUuid.get(dupedUuid).duped = true;

                            article.duped = true;
                            Tema1.articlesByTitle.put(article.title, article);
                            Tema1.articlesByUuid.put(article.uuid, article);
                        } else if (Tema1.articlesByUuid.containsKey(article.uuid)) {
                            String dupedTitle = Tema1.articlesByUuid.get(article.uuid).title;
                            Tema1.articlesByTitle.get(dupedTitle).duped = true;

                            article.duped = true;
                            Tema1.articlesByTitle.put(article.title, article);
                            Tema1.articlesByUuid.put(article.uuid, article);
                        } else {
                            Tema1.articlesByTitle.put(article.title, article);
                            Tema1.articlesByUuid.put(article.uuid, article);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Eroare la procesarea fișierului: " + filePath);
                e.printStackTrace();
            }
        }
    }

    private void writeCategories() {
        while (true) {
            String category = Tema1.categoriesQueue.poll();
            if (category == null) {
                break;
            }
            if (Tema1.articlesByCategory.containsKey(category)
                    && !Tema1.articlesByCategory.get(category).isEmpty()) {
                ArrayList<String> articles = Tema1.articlesByCategory.get(category).stream()
                        .filter(uuid -> !Tema1.articlesByUuid.get(uuid).duped)
                        .sorted()
                        .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

                String fileName = category.replaceAll(" ", "_")
                        .replaceAll(",", "") + ".txt";
                Path outPath = Path.of(fileName);
                try (BufferedWriter writer =
                             Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
                    for (String uuid : articles) {
                        writer.write(uuid);
                        writer.newLine();
                    }
                } catch (Exception e) {
                    System.err.println("Eroare la scrierea fișierului: " + fileName);
                    e.printStackTrace();
                }
            }
        }
    }

    private void writeLanguages() {
        while (true) {
            String language = Tema1.languagesQueue.poll();
            if (language == null) {
                break;
            }
            if (Tema1.articlesByLanguage.containsKey(language) &&
                    !Tema1.articlesByLanguage.get(language).isEmpty()) {
                ArrayList<String> articles = Tema1.articlesByLanguage.get(language).stream()
                        .filter(uuid -> !Tema1.articlesByUuid.get(uuid).duped)
                        .sorted()
                        .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

                Path outPath = Path.of(language + ".txt");
                try (BufferedWriter writer =
                             Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
                    for (String uuid : articles) {
                        writer.write(uuid);
                        writer.newLine();
                    }
                } catch (Exception e) {
                    System.err.println("Eroare la scrierea fișierului: " + language + ".txt");
                    e.printStackTrace();
                }
            }
        }
    }

    private Article parseArticle(JsonNode articleNode) {
        String uuid = articleNode.path("uuid").asText();
        String title = articleNode.path("title").asText();
        String author = articleNode.path("author").asText();
        String url = articleNode.path("url").asText();
        String text = articleNode.path("text").asText();
        String published = articleNode.path("published").asText();
        String language = articleNode.path("language").asText();

        ArrayList<String> categories = new ArrayList<>();
        JsonNode categoriesNode = articleNode.path("categories");
        if (categoriesNode.isArray()) {
            for (JsonNode categoryNode : categoriesNode) {
                categories.add(categoryNode.asText());
            }
        }

        return new Article(uuid, title, author, url, text, published, language, categories);
    }
}
