# syntax=docker/dockerfile:1.7
#
# One image for three roles: `bootstrap` (one-shot init), `serve` (demo UI/API) and `verify`
# (containerised acceptance checks). All compilation happens here, so the host needs only
# Docker. Base images are pinned by digest (see README "Tested compatibility matrix").

# ---- the pinned Flink distribution: source of the official Flink logo (see THIRD_PARTY_ASSETS.md)
FROM apache/fluss-quickstart-flink:1.20-1.0.0@sha256:70e67eeea65929cfb939aed89146212cfbda6dd4709aae9764fea1209072fcf9 AS flinkdist

# ---- build: compile all modules, package the demo app and the Flink job jar ----------------
FROM maven:3.9.9-eclipse-temurin-17@sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc AS build
WORKDIR /logo
COPY --from=flinkdist /opt/flink/lib/flink-dist-1.20.0.jar /tmp/flink-dist.jar
RUN jar xf /tmp/flink-dist.jar web/assets/images/flink.svg \
 && mv web/assets/images/flink.svg /logo/apache-flink.svg && rm -rf web /tmp/flink-dist.jar
WORKDIR /src
COPY pom.xml ./
COPY common/pom.xml common/
COPY flink-job/pom.xml flink-job/
COPY demo-app/pom.xml demo-app/
COPY common/src common/src
COPY flink-job/src flink-job/src
COPY demo-app/src demo-app/src
COPY scenarios scenarios
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -DskipTests package

# ---- test: unit tests only (no services). CI: docker build --target test -f docker/app/Dockerfile .
FROM build AS test
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp verify

# ---- runtime -----------------------------------------------------------------------------
FROM eclipse-temurin:17-jre@sha256:e04fa72be13b257a59fdec6dc14af78c8f80f035d3c1ed4e03e45552a231d4c8 AS runtime
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --home-dir /app --no-create-home app \
 && mkdir -p /app /status \
 && chown app:app /status
WORKDIR /app
COPY --from=build /src/demo-app/target/demo-app.jar /app/demo-app.jar
COPY --from=build /src/demo-app/target/lib /app/lib
COPY --from=build /src/flink-job/target/flink-job.jar /app/flink-job.jar
COPY web /app/web
COPY assets /app/assets
COPY --from=build /logo/apache-flink.svg /app/assets/logos/apache-flink.svg
COPY scenarios /app/scenarios
USER app
EXPOSE 8090
ENTRYPOINT ["java", "-cp", "/app/demo-app.jar:/app/lib/*", "demo.paymentrisk.app.Main"]
CMD ["serve"]
