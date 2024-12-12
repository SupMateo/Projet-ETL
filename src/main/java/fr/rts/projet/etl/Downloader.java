package fr.rts.projet.etl;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.Objects;

public class Downloader {
    private final String datasetUrl;

    public Downloader(String datasetUrl) {
        this.datasetUrl = datasetUrl;
    }

    public void downloadAndUnzip() {
        try {
            String zipUrl = extractDownloadLink(datasetUrl);
            if (zipUrl == null) {
                System.err.println("Download link not found !");
                return;
            }

            File downloadedZip = downloadFile(zipUrl, "dataset.zip");
            System.out.println("File downloaded : " + downloadedZip.getAbsolutePath());

            File outputDir = new File("unzipped");
            unzipFile(downloadedZip, outputDir);
            System.out.println("File extracted in : " + outputDir.getAbsolutePath());

            extractNestedZips(outputDir);
            System.out.println("Nested ZIPs extraction complete.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void extractNestedZips(File directory) throws IOException {
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (file.isDirectory()) {
                extractNestedZips(file);
            } else if (file.getName().endsWith(".zip")) {
                File nestedOutputDir = new File(file.getParent(), file.getName().replace(".zip", ""));
                System.out.println("Extracting nested ZIP: " + file.getAbsolutePath());
                unzipFile(file, nestedOutputDir);
                extractNestedZips(nestedOutputDir);
            }
        }
    }
    private String extractDownloadLink(String pageUrl) throws IOException {
        Document doc = Jsoup.connect(pageUrl).get();
        for (Element link : doc.select("a[href]")) {
            String href = link.attr("href");
            if (href.endsWith(".zip")) {
                return href.startsWith("http") ? href : new URL(new URL(pageUrl), href).toString();
            }
        }
        return null;
    }

    private File downloadFile(String fileUrl, String outputFileName) throws IOException {
        URL url = new URL(fileUrl);
        File outputFile = new File(outputFileName);

        try (InputStream in = url.openStream();
             FileOutputStream fos = new FileOutputStream(outputFile)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
        return outputFile;
    }

    private void unzipFile(File zipFile, File outputDir) throws IOException {
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Unzip
        try (ZipFile zip = new ZipFile(zipFile)) {
            Enumeration<ZipArchiveEntry> entries = zip.getEntries();

            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                File outputFile = new File(outputDir, entry.getName());

                if (entry.isDirectory()) {
                    outputFile.mkdirs();
                } else {
                    try (InputStream is = zip.getInputStream(entry);
                         OutputStream os = new FileOutputStream(outputFile)) {
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            os.write(buffer, 0, bytesRead);
                        }
                    }
                }
            }
        }
    }
}
