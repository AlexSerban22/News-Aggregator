import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Tema1 {
    public static BlockingQueue<String> articlesQueue;
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
    public static ConcurrentHashMap<String, Integer> authorArticles = new ConcurrentHashMap<>();
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
        String articlesFile = args[1];
        String inputsFile = args[2];

        Thread[] threads = new Thread[threadNumber];

        // Read articles file and populate the articlesQueue
        try {
            Scanner scanner = new Scanner(new File(articlesFile));

            if (scanner.hasNextInt()) {
                int numberOfFiles = scanner.nextInt();
                scanner.nextLine();

                articlesQueue = new ArrayBlockingQueue<>(numberOfFiles);

                for (int i = 0; i < numberOfFiles; i++) {
                    if (scanner.hasNextLine()) {
                        String filePath = scanner.nextLine().trim();
                        articlesQueue.add(filePath);
                    }
                }
            }
            scanner.close();

        } catch (FileNotFoundException e) {
            System.err.println("Fisierul de intrare nu a fost gasit: " + articlesFile);
            e.printStackTrace();
            return;
        }

        // Read inputs file and get categories, languages and keywords
        try {
            Scanner scanner = new Scanner(new File(inputsFile));

            // Read linking words
            if (scanner.hasNextInt()) {
                int numberOfFiles = scanner.nextInt();
                scanner.nextLine();

                for (int i = 0; i < numberOfFiles; i++) {
                    if (scanner.hasNextLine()) {
                        String filePath = scanner.nextLine().trim();
                        readInputFile(filePath, sets.get(i));
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("Fisierul de intrare nu a fost gasit: " + inputsFile);
            e.printStackTrace();
            return;
        }

        for (String category : categories) {
            articlesByCategory.put(category, new LinkedBlockingQueue<>());
            categoryCounts.put(category, 0);
        }
        for (String language : languages) {
            articlesByLanguage.put(language, new LinkedBlockingQueue<>());
            languageCounts.put(language, 0);
        }

        // populate categoriesQueue and languagesQueue
        categoriesQueue = new ArrayBlockingQueue<>(categories.size());
        categoriesQueue.addAll(categories);
        languagesQueue = new ArrayBlockingQueue<>(languages.size());
        languagesQueue.addAll(languages);
        articlesRead = new int[threadNumber];

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
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

        Path outPath = Path.of("all_articles.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
            for (Article a : uniques) {
                writer.write(a.uuid + " " + a.published + "\n");
            }
        } catch (Exception e) {
            System.err.println("Eroare la scrierea fisierului: all_articles.txt");
            e.printStackTrace();
        }

        Map<String, Integer> keywordCount = uniques.stream()
                .filter(a -> a.language.equals("english"))
                .map(a -> a.text.toLowerCase().replaceAll("[^a-z ]", "").split(" "))
                .map(words -> Arrays.stream(words).collect(Collectors.toSet()))
                .map(wordSet -> wordSet.stream()
                        .filter(w -> !linkingWords.contains(w))
                        .collect(Collectors.toSet()))
                .flatMap(Set::stream)
                .collect(Collectors.toMap(w -> w, w -> 1, Integer::sum))
                .entrySet().stream()
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


        Path outPathKeywords = Path.of("keywords_count.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outPathKeywords)) {
            for (Map.Entry<String, Integer> entry : keywordCount.entrySet()) {
                writer.write(entry.getKey() + " " + entry.getValue() + "\n");
            }
        } catch (Exception e) {
            System.err.println("Eroare la scrierea fișierului: keywords_count.txt");
            e.printStackTrace();
        }

        int totalArticles = Arrays.stream(articlesRead).sum();
        int duplicatesCount = totalArticles - uniques.size();
        int uniquesCount = uniques.size();
        String bestAuthor = authorArticles.entrySet().stream()
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .map(Map.Entry::getKey)
                .orElse("");
        int bestAuthorArticles = authorArticles.getOrDefault(bestAuthor, 0);
        String bestCategory = categoryCounts.entrySet().stream()
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .map(Map.Entry::getKey)
                .orElse("");
        int bestCategoryArticles = categoryCounts.getOrDefault(bestCategory, 0);
        bestCategory = bestCategory.replaceAll(",", "").replaceAll(" ", "_");
        String bestLanguage = languageCounts.entrySet().stream()
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .map(Map.Entry::getKey)
                .orElse("");
        int bestLanguageArticles = languageCounts.getOrDefault(bestLanguage, 0);
        Article mostRecentArticle = uniques.stream()
                .max((a, b) -> {
                    int cmp = a.published.compareTo(b.published);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return b.uuid.compareTo(a.uuid);
                })
                .orElse(null);

        Map.Entry<String, Integer> bestKeyword = keywordCount.keySet().stream()
                .map(k -> Map.entry(k, keywordCount.get(k)))
                .max((e1, e2) -> {
                    int cmp = e1.getValue().compareTo(e2.getValue());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return e2.getKey().compareTo(e1.getKey());
                })
                .orElse(Map.entry("", 0));

        Path outPathStats = Path.of("reports.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outPathStats)) {
            writer.write("duplicates_found - " + duplicatesCount + "\n");
            writer.write("unique_articles - " + uniquesCount + "\n");
            writer.write("best_author - " + bestAuthor + " " + bestAuthorArticles + "\n");
            writer.write("top_language - " + bestLanguage + " " + bestLanguageArticles + "\n");
            writer.write("top_category - " + bestCategory + " " + bestCategoryArticles + "\n");
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
