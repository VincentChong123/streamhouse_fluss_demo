package demo.paymentrisk.app.scenario;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/** A scenario fixture from {@code scenarios/*.json}. */
public final class Scenario {
    public String id;
    public int order;
    public String title;
    public String variant;
    public String summary;
    public boolean ui;
    public String supplierId;
    public String paymentId;
    public ObjectNode expectedFinal;
    public ObjectNode source;
    public final List<Step> steps = new ArrayList<>();

    public static final class Step {
        public String id;
        public String label;
        public String eventType;
        public long businessOffsetSeconds;
        public String caption;
        public String notice;
        public ObjectNode payload;
        /** Re-send the exact record of an earlier step (same event_id): a duplicate delivery. */
        public String duplicateOf;
        public ObjectNode expect;
    }

    public long maxOffsetSeconds() {
        long max = 0;
        for (Step s : steps) {
            max = Math.max(max, s.businessOffsetSeconds);
        }
        return max;
    }

    public static Scenario fromJson(JsonNode n) {
        Scenario s = new Scenario();
        s.source = (ObjectNode) n;
        s.id = req(n, "id");
        s.order = n.path("order").asInt(100);
        s.title = req(n, "title");
        s.variant = n.path("variant").asText(s.title);
        s.summary = n.path("summary").asText("");
        s.ui = n.path("ui").asBoolean(true);
        s.supplierId = req(n, "supplier_id");
        s.paymentId = n.path("payment_id").asText(null);
        s.expectedFinal = n.has("expected_final") ? (ObjectNode) n.get("expected_final") : null;
        for (JsonNode j : n.path("steps")) {
            Step st = new Step();
            st.id = req(j, "id");
            st.label = j.path("label").asText(st.id);
            st.duplicateOf = j.path("duplicate_of").asText(null);
            st.eventType = st.duplicateOf == null ? req(j, "event_type") : j.path("event_type").asText(null);
            st.businessOffsetSeconds = j.path("business_offset_seconds").asLong(0);
            st.caption = j.path("caption").asText("");
            st.notice = j.path("notice").asText("");
            st.payload = j.has("payload") ? (ObjectNode) j.get("payload") : null;
            st.expect = j.has("expect") ? (ObjectNode) j.get("expect") : null;
            s.steps.add(st);
        }
        if (s.steps.isEmpty()) {
            throw new IllegalArgumentException("scenario " + s.id + " has no steps");
        }
        return s;
    }

    private static String req(JsonNode n, String f) {
        String v = n.path(f).asText(null);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("scenario field '" + f + "' is required");
        }
        return v;
    }
}
