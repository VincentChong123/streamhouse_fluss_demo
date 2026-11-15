package demo.paymentrisk.app.scenario;

import demo.paymentrisk.common.Json;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Loads every {@code *.json} scenario fixture from a directory. */
public final class ScenarioCatalog {
    private final Map<String, Scenario> byId = new LinkedHashMap<>();

    public ScenarioCatalog(Path dir) throws IOException {
        List<Scenario> all = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".json")).sorted()::iterator) {
                try {
                    all.add(Scenario.fromJson(Json.mapper().readTree(Files.readString(p))));
                } catch (Exception e) {
                    throw new IOException("Invalid scenario fixture " + p.getFileName() + ": " + e.getMessage(), e);
                }
            }
        }
        all.sort(Comparator.comparingInt((Scenario s) -> s.order).thenComparing(s -> s.id));
        for (Scenario s : all) {
            byId.put(s.id, s);
        }
    }

    public Scenario get(String id) {
        return byId.get(id);
    }

    public List<Scenario> all() {
        return new ArrayList<>(byId.values());
    }
}
