package tp2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Estatísticas e CSV independentes da configuração regional do computador. */
final class Metrics {
    private Metrics() {
    }

    static double median(List<Double> samples) {
        if (samples.isEmpty())
            return 0;

        List<Double> values = new ArrayList<>(samples);
        values.sort(Double::compare);

        int middle = values.size() / 2;

        return values.size() % 2 == 1 ? values.get(middle) : values.get(middle - 1) / 2 + values.get(middle) / 2;
    }

    /** Percentil pelo critério de posto mais próximo: ceil(p*N). */
    static double percentile(List<Double> samples, double fraction) {
        if (samples.isEmpty())
            return 0;

        List<Double> values = new ArrayList<>(samples);
        values.sort(Double::compare);

        int position = Math.max(0, (int) Math.ceil(fraction * values.size()) - 1);

        return values.get(position);
    }

    static String number(double value) {
        return String.format(Locale.ROOT, "%.6f", value);
    }

    static String csvCell(Object value) {
        String text = String.valueOf(value);
        return text.contains(",") || text.contains("\"") || text.contains("\n")
                ? "\"" + text.replace("\"", "\"\"") + "\""
                : text;
    }

    static void writeCsv(Path path, Map<String, String> row) throws IOException {
        if (path.getParent() != null)
            Files.createDirectories(path.getParent());

        String header = String.join(",", row.keySet());
        String data = row.values().stream().map(Metrics::csvCell).collect(Collectors.joining(","));

        Files.writeString(path, header + "\n" + data + "\n", StandardCharsets.UTF_8);
    }
}
