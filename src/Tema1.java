import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Tema1 {
    public static BlockingQueue<String> articleFilesQueue;
    public static ConcurrentHashMap<String, Article> articlesByTitle = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Article> articlesByUuid = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, LinkedBlockingQueue<String>> articlesByCategory = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, LinkedBlockingQueue<String>> articlesByLanguage = new ConcurrentHashMap<>();
    public static HashSet<String> categories = new HashSet<>();
    public static HashSet<String> languages = new HashSet<>();
    public static HashSet<String> linkingWords = new HashSet<>();
    public static ArrayList<HashSet<String>> sets = new ArrayList<>();
    public static CyclicBarrier barrier;
    public static BlockingQueue<String> categoriesQueue;
    public static BlockingQueue<String> languagesQueue;
    public static int[] articlesRead;
    public static BlockingQueue<Article> articlesQueue;
    public static Map<String, Integer> keywordCounts = new ConcurrentHashMap<>();
    public static Map<String, Integer> authorArticles = new ConcurrentHashMap<>();
    public static Map<String, Integer> categoryCounts = new HashMap<>();
    public static Map<String, Integer> languageCounts = new HashMap<>();


    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java Tema1 <number_of_threads> <articles_file> <inputs_file>");
            return;
        }

        sets.add(languages);
        sets.add(categories);
        sets.add(linkingWords);

        int threadNumber = Integer.parseInt(args[0]);
        Thread[] threads = new Thread[threadNumber];

        String articlesFile = args[1];

        File articlesF = new File(articlesFile);
        Path articlesBaseDir = articlesF.getParentFile().toPath();

        try (Scanner scanner = new Scanner(articlesF)) {
            if (scanner.hasNextInt()) {
                int numberOfFiles = scanner.nextInt();
                scanner.nextLine();

                articleFilesQueue = new ArrayBlockingQueue<>(numberOfFiles);

                for (int i = 0; i < numberOfFiles && scanner.hasNextLine(); i++) {
                    String relativePath = scanner.nextLine().trim();
                    Path resolved = articlesBaseDir.resolve(relativePath).normalize();
                    articleFilesQueue.add(resolved.toString());
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("Fisierul de intrare nu a fost gasit: " + articlesFile);
            e.printStackTrace();
            return;
        }

        // Read inputs file and get categories, languages and keywords
        String inputsFile = args[2];

        File inputsF = new File(inputsFile);
        Path inputsBaseDir = inputsF.getParentFile().toPath();

        try (Scanner scanner = new Scanner(inputsF)) {
            if (scanner.hasNextInt()) {
                int numberOfFiles = scanner.nextInt();
                scanner.nextLine();

                for (int i = 0; i < numberOfFiles && scanner.hasNextLine(); i++) {
                    String relativePath = scanner.nextLine().trim();
                    Path resolved = inputsBaseDir.resolve(relativePath).normalize();
                    readInputFile(resolved.toString(), sets.get(i));
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("Fisierul de intrare nu a fost gasit: " + inputsFile);
            e.printStackTrace();
            return;
        }

        for (String category : categories) {
            articlesByCategory.put(category, new LinkedBlockingQueue<>());
        }
        for (String language : languages) {
            articlesByLanguage.put(language, new LinkedBlockingQueue<>());
        }

        // populate categoriesQueue and languagesQueue
        categoriesQueue = new ArrayBlockingQueue<>(categories.size());
        categoriesQueue.addAll(categories);
        languagesQueue = new ArrayBlockingQueue<>(languages.size());
        languagesQueue.addAll(languages);
        articlesRead = new int[threadNumber];
        articlesQueue = new LinkedBlockingQueue<>();

        barrier = new CyclicBarrier(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            threads[i] = new Thread(new MyThread(i));
            threads[i].start();
        }

        for (int i = 0; i < threadNumber; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ArrayList<Article> uniques = articlesByTitle.values().stream()
                .filter(article -> !article.duped)
                .sorted((a, b) -> {
                    int cmp = b.published.compareTo(a.published);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return a.uuid.compareTo(b.uuid);
                })
                .collect(Collectors.toCollection(ArrayList::new));

        Path outPath = Path.of("./all_articles.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
            for (Article a : uniques) {
                writer.write(a.uuid + " " + a.published + "\n");
            }
        } catch (Exception e) {
            System.err.println("Eroare la scrierea fisierului: all_articles.txt");
            e.printStackTrace();
        }

        HashMap <String, Integer> sortedKeywordCounts = keywordCounts.entrySet().stream()
                .sorted((e1, e2) -> {
                    int cmp = e2.getValue().compareTo(e1.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e1.getKey().compareTo(e2.getKey());
                })
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        Path outPathKeywords = Path.of("./keywords_count.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outPathKeywords)) {
            for (Map.Entry<String, Integer> entry : sortedKeywordCounts.entrySet()) {
                writer.write(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }
        } catch (Exception e) {
            System.err.println("Eroare la scrierea fișierului: keywords_count.txt");
            e.printStackTrace();
        }

        // Calculate statistics
        int totalArticles = Arrays.stream(articlesRead).sum();
        // 1. Number of duplicate articles
        int duplicatesCount = totalArticles - uniques.size();
        // 2. Number of unique articles
        int uniquesCount = uniques.size();

        // 3. Author with most articles
        Map.Entry<String, Integer> bestAuthor = authorArticles.entrySet().stream()
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .orElse(Map.entry("", 0));

        // 4. Category with most articles
        for (String category : categories) {
            var uuids = articlesByCategory.get(category);
            if (uuids == null) {
                categoryCounts.put(category, 0);
                continue;
            }

            int cnt = (int) uuids.stream()
                    .filter(uuid -> {
                        Article a = articlesByUuid.get(uuid);
                        return a != null && !a.duped;
                    })
                    .distinct()
                    .count();

            categoryCounts.put(category, cnt);
        }

        Map.Entry<String, Integer> bestCategory = categoryCounts.entrySet().stream()
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .orElse(Map.entry("", 0));
        String bestCategoryName = bestCategory.getKey()
                .replaceAll(",", "").replaceAll(" ", "_");

        // 5. Language with most articles
        for (String language : languages) {
            int count = articlesByLanguage.get(language).stream()
                    .filter(uuid -> !articlesByUuid.get(uuid).duped)
                    .distinct()
                    .toArray().length;
            languageCounts.put(language, count);
        }

        Map.Entry<String, Integer> bestLanguage = languageCounts.entrySet().stream()
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .orElse(Map.entry("", 0));

        // 6. Most recent article
        Article mostRecentArticle = uniques.stream()
                .max((a, b) -> {
                    int cmp = a.published.compareTo(b.published);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return b.uuid.compareTo(a.uuid);
                })
                .orElse(null);

        // 7. Most common keyword
        Map.Entry<String, Integer> bestKeyword = sortedKeywordCounts.entrySet().stream()
                .findFirst()
                .orElse(Map.entry("", 0));

        Path outPathStats = Path.of("./reports.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outPathStats)) {
            writer.write("duplicates_found - " + duplicatesCount + "\n");
            writer.write("unique_articles - " + uniquesCount + "\n");
            writer.write("best_author - " + bestAuthor.getKey() + " " + bestAuthor.getValue() + "\n");
            writer.write("top_language - " + bestLanguage.getKey() + " " + bestLanguage.getValue() + "\n");
            writer.write("top_category - " + bestCategoryName + " " + bestCategory.getValue() + "\n");
            writer.write("most_recent_article - " + mostRecentArticle.published + " " + mostRecentArticle.url + "\n");
            writer.write("top_keyword_en - " + bestKeyword.getKey() + " " + bestKeyword.getValue() + "\n");
        } catch (Exception e) {
            System.err.println("Eroare la scrierea fișierului: reports.txt");
            e.printStackTrace();
        }

    }

    public static void readInputFile(String inputFile, HashSet<String> items) {
        try {
            Scanner scanner = new Scanner(new File(inputFile));

            if (scanner.hasNextInt()) {
                int numberOfFiles = scanner.nextInt();
                scanner.nextLine();

                for (int i = 0; i < numberOfFiles; i++) {
                    if (scanner.hasNextLine()) {
                        String item = scanner.nextLine().trim();
                        items.add(item);
                    }
                }
            }
            scanner.close();

        } catch (FileNotFoundException e) {
            System.err.println("Fisierul de intrare nu a fost gasit: " + inputFile);
            e.printStackTrace();
        }
    }
}
