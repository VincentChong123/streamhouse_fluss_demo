package demo.paymentrisk.app.bootstrap;

import demo.paymentrisk.common.Json;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Progress of the one-shot bootstrap, written to a shared volume so the UI can show it. */
public final class BootstrapStatus {
    public String state = "RUNNING"; // RUNNING | DONE | FAILED
    public String startedAt = Instant.now().toString();
    public String updatedAt = startedAt;
    public List<Step> steps = new ArrayList<>();

    public static final class Step {
        public String id;
        public String label;
        public String state = "PENDING"; // PENDING | RUNNING | DONE | FAILED
        public String detail;
        public String hint;
        public int attempts;
    }

    public Step step(String id, String label) {
        Step s = new Step();
        s.id = id;
        s.label = label;
        steps.add(s);
        return s;
    }

    public synchronized void write(Path file) {
        updatedAt = Instant.now().toString();
        try {
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp, Json.write(this));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            System.err.println("WARN: could not write bootstrap status to " + file + ": " + e);
        }
    }

    public static BootstrapStatus read(Path file) {
        try {
            return Files.exists(file) ? Json.read(Files.readString(file), BootstrapStatus.class) : null;
        } catch (Exception e) {
            return null;
        }
    }
}
