import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

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

        processArticles();
        writeCategories();
        writeLanguages();
    }

    private void processFiles() {
        while (true) {
            String filePath = Tema1.articleFilesQueue.poll();
            if (filePath == null) {
                break;
            }
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode rootNode = mapper.readTree(new File(filePath));
                if (rootNode.isArray()) {
                    for (JsonNode articleNode : rootNode) {
                        Article article = parseArticle(articleNode);
                        Tema1.articlesQueue.add(article);
                        Tema1.articlesRead[threadId]++;

                        if (Tema1.articlesByLanguage.containsKey(article.language))
                            Tema1.articlesByLanguage.get(article.language).add(article.uuid);
                        for (String category : article.categories) {
                            if (Tema1.articlesByCategory.containsKey(category)) {
                                Tema1.articlesByCategory.get(category).add(article.uuid);
                            }
                        }

                        // 1. Înregistrăm articolul după titlu
                        Article existingByTitle = Tema1.articlesByTitle.putIfAbsent(article.title, article);
                        // 2. Înregistrăm articolul după uuid
                        Article existingByUuid = Tema1.articlesByUuid.putIfAbsent(article.uuid, article);

                        // 3. Dacă exista deja un articol cu același titlu,
                        //    îl marcăm pe ăla și pe ăsta ca duplicate
                        if (existingByTitle != null) {
                            existingByTitle.duped = true;
                            article.duped = true;
                        }

                        // 4. Dacă exista deja un articol cu același uuid,
                        //    îl marcăm și pe ăla, și pe ăsta ca duplicate
                        if (existingByUuid != null) {
                            existingByUuid.duped = true;
                            article.duped = true;
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
                        .distinct()
                        .sorted()
                        .collect(Collectors.toCollection(ArrayList::new));

                String fileName = category.replaceAll(" ", "_")
                        .replaceAll(",", "") + ".txt";
                Path outPath = Path.of("./" + fileName);
                try (BufferedWriter writer =
                             Files.newBufferedWriter(outPath)) {
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

                Path outPath = Path.of("./" + language + ".txt");
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

    private void processArticles() {
        while (true) {
            Article article = Tema1.articlesQueue.poll();
            if (article == null) {
                break;
            }
            if (article.duped) {
                continue;
            }
            Tema1.authorArticles.merge(article.author, 1, Integer::sum);

            if (!article.language.equals("english")) {
                continue;
            }
            String text = article.text.replaceAll("\\s+", " ").trim();
            String[] words = Arrays.stream(text.toLowerCase().split(" "))
                    .map(word -> word.replaceAll("[^a-z]", ""))
                    .distinct()
                    .toArray(String[]::new);
            for (String word : words) {
                if (!Tema1.linkingWords.contains(word) && !word.isEmpty()) {
                    Tema1.keywordCounts.merge(word, 1, Integer::sum);
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
