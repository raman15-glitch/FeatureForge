package com.featureforge.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;

public final class AtomicStageRunner {

    public static void writeParquetAtomically(
            Dataset<Row> df,
            Path targetDir,
            Path configPath,
            String gitCommit) throws Exception {

        Path tempDir = targetDir.resolveSibling(targetDir.getFileName() + "_tmp_" + System.currentTimeMillis());
        if (Files.exists(tempDir)) {
            deleteRecursively(tempDir);
        }

        // Write deterministic sorted Parquet to temporary path
        df.write().mode(SaveMode.Overwrite).parquet(tempDir.toString());
        long rowCount = df.count();
        String configHash = sha256File(configPath);
        String schemaHash = sha256String(df.schema().json());

        String manifestJson = String.format(
                "{\n  \"rowCount\": %d,\n  \"configHash\": \"%s\",\n  \"schemaHash\": \"%s\",\n  \"gitCommit\": \"%s\"\n}",
                rowCount, configHash, schemaHash, gitCommit
        );

        Files.writeString(tempDir.resolve("manifest.json"), manifestJson, StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("_SUCCESS"), "", StandardCharsets.UTF_8);

        if (Files.exists(targetDir)) {
            deleteRecursively(targetDir);
        }
        Files.move(tempDir, targetDir, StandardCopyOption.ATOMIC_MOVE);
    }

    public static void assertCompletedStage(Path stageDir) {
        if (!Files.exists(stageDir.resolve("_SUCCESS")) || !Files.exists(stageDir.resolve("manifest.json"))) {
            throw new IllegalStateException("Stage directory is incomplete or missing _SUCCESS marker: " + stageDir);
        }
    }

    private static String sha256File(Path path) throws Exception {
        return sha256Bytes(Files.readAllBytes(path));
    }

    private static String sha256String(String s) throws Exception {
        return sha256Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256Bytes(byte[] bytes) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(md.digest(bytes));
    }

    private static void deleteRecursively(Path root) throws Exception {
        try (var stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.delete(p); } catch (Exception ignored) {}
            });
        }
    }
}