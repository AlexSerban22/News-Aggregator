import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.*;

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
        }
        for (String language : languages) {
            articlesByLanguage.put(language, new LinkedBlockingQueue<>());
        }

        // populate categoriesQueue and languagesQueue
        categoriesQueue = new ArrayBlockingQueue<>(categories.size());
        categoriesQueue.addAll(categories);
        languagesQueue = new ArrayBlockingQueue<>(languages.size());
        languagesQueue.addAll(languages);

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

        ArrayList<String> nonDupTitles = articlesByTitle.entrySet().stream()
                .filter(e -> !e.getValue().duped)
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

        System.out.println("non-duplicated titles:");
        for (String title : nonDupTitles) {
            System.out.println(title);
        }

//        for (Map.Entry<String, LinkedBlockingQueue<String>> entry : articlesByCategory.entrySet()) {
//            entry.setValue(entry.getValue().stream()
//                    .filter(uuid -> !articlesByUuid.get(uuid).duped)
//                    .collect(java.util.stream.Collectors.toCollection(LinkedBlockingQueue::new)));
//        }
//        for (Map.Entry<String, LinkedBlockingQueue<String>> entry : articlesByLanguage.entrySet()) {
//            entry.setValue(entry.getValue().stream()
//                    .filter(uuid -> !articlesByUuid.get(uuid).duped)
//                    .collect(java.util.stream.Collectors.toCollection(LinkedBlockingQueue::new)));
//        }
//        System.out.println("Categories:");
//        for (String category : categories) {
//            System.out.println(category.replaceAll(" ", "_").replaceAll(",", "")
//                    + ".txt: ");
//            for (String uuid : articlesByCategory.get(category)) {
//                System.out.println(" - " + uuid);
//            }
//        }
//        System.out.println();
//        System.out.println("Languages:");
//        for (String language : languages) {
//            System.out.println(language + ".txt: ");
//            for (String uuid : articlesByLanguage.get(language)) {
//                System.out.println(" - " + uuid);
//            }
//        }
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
