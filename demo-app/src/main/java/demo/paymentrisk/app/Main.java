package demo.paymentrisk.app;

import demo.paymentrisk.app.bootstrap.Bootstrapper;

import java.util.Arrays;

/** Entry point for every container role that uses the app image. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        String cmd = args.length == 0 ? "serve" : args[0];
        String[] rest = Arrays.copyOfRange(args, Math.min(1, args.length), args.length);
        AppConfig cfg = new AppConfig();
        int code;
        switch (cmd) {
            case "bootstrap":
                code = new Bootstrapper(cfg).run();
                break;
            case "slice":
                code = SliceCheck.run(cfg);
                break;
            case "serve":
                DemoServer.start(cfg);
                return; // keeps running
            case "verify":
                code = demo.paymentrisk.app.verify.Verifier.run(cfg, rest);
                break;
            default:
                System.err.println("usage: (bootstrap | serve | slice | verify [--phase NAME])");
                code = 2;
        }
        System.exit(code);
    }
}
