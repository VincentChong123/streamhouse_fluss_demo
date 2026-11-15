package demo.paymentrisk.app;

import demo.paymentrisk.app.verify.Verifier;

/**
 * Quick end-to-end proof of the four stages against the running stack, through the demo app's
 * own HTTP API (the same calls the browser makes).
 *
 * <pre>docker compose run --rm e2e slice</pre>
 */
public final class SliceCheck {
    private SliceCheck() {}

    public static int run(AppConfig cfg) throws Exception {
        return Verifier.run(cfg, new String[] {"--phase", "stages"});
    }
}
