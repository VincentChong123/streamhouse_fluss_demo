# Third-party assets and provenance

The Apache-2.0 license of this repository covers the original code and docs only. It does not
relicense the items below. Apache, Apache Kafka, Apache Flink, Apache Fluss and their logos are
trademarks of The Apache Software Foundation (ASF). They are used here only to refer to the
projects, in line with the [ASF trademark policy](https://www.apache.org/foundation/marks/). This
demo is not affiliated with or endorsed by the ASF or any vendor.

## Logos

| File | What | Where it came from | Terms | Verified |
| --- | --- | --- | --- | --- |
| `assets/logos/apache-fluss.svg` | Apache Fluss logo (white wordmark, for dark backgrounds) | Copied byte-for-byte from the image labelled "Official Apache Fluss logo" embedded in the author's meet-up deck (`Fresh_Context_Streamhouse_Fluss_Meetup.html`, not part of this repo). The file keeps its original ASF Apache-2.0 header. The deck links to `https://fluss.apache.org/img/logo/svg/` as the origin. SHA-256 `0520369620808858bba8d93b2a95e01f03e10c8feebfc0312528f6fb6e8eb956`. | Apache-2.0 (file header); ASF trademark | **Before publication:** compare with the file on fluss.apache.org (it could not be downloaded from the build environment). |
| `assets/logos/apache-kafka.png` | Apache Kafka logo (117×65 PNG) | Copied byte-for-byte from the image labelled "Official Apache Kafka logo" in the same deck. SHA-256 `3cfcbb2acf90114ffc5b9287a336487c4872be230e4553bdca7f2a09be6a7d33`. | ASF trademark | **Before publication:** replace with or confirm against the file at https://kafka.apache.org/ (logos page). |
| Flink logo (`/app/assets/logos/apache-flink.svg` inside the image, not committed) | Apache Flink squirrel logo | Extracted **at image build time** from `web/assets/images/flink.svg` inside `flink-dist-1.20.0.jar` of the Apache Flink 1.20.0 binary distribution shipped in `apache/fluss-quickstart-flink:1.20-1.0.0@sha256:70e67eeea6…` (see `docker/app/Dockerfile`). | Apache-2.0 (Apache Flink release); ASF trademark | Exact upstream file from an official ASF release. |

No speaker photo, biography, employer branding or other slide content from the deck is used. The
dashboard copies only the deck's colour palette and font stack (system fonts, no web fonts).

## Container images (pulled at runtime, not redistributed by this repo)

| Image | Pinned digest | License |
| --- | --- | --- |
| `apache/kafka:3.9.1` | `sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed` | Apache-2.0 |
| `zookeeper:3.9.2` | `sha256:43d39109cbd4de394742efbfc162a8a0a93731e806374b71f3865f4ef4460a56` | Apache-2.0 |
| `apache/fluss:1.0.0` | `sha256:ff461b45438033da4fd1c2556d3f978f3603bb3632fe075c2bd57388339a58cb` | Apache-2.0 |
| `apache/fluss-quickstart-flink:1.20-1.0.0` (Flink 1.20.0 + Fluss connector) | `sha256:70e67eeea65929cfb939aed89146212cfbda6dd4709aae9764fea1209072fcf9` | Apache-2.0 (bundles third-party jars; see the image) |
| `maven:3.9.9-eclipse-temurin-17` (build only) | `sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc` | Apache-2.0 / GPLv2+CE (JDK) |
| `eclipse-temurin:17-jre` | `sha256:e04fa72be13b257a59fdec6dc14af78c8f80f035d3c1ed4e03e45552a231d4c8` | GPLv2 with Classpath Exception |
| `python:3.12-slim` (python-runner base) | `sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b` | PSF / Debian packages |

## Libraries (downloaded from Maven Central during the image build)

| Artifact | Version | Where it ends up | License |
| --- | --- | --- | --- |
| `org.apache.fluss:fluss-client` | 1.0.0 | app image `lib/` | Apache-2.0 |
| `org.apache.kafka:kafka-clients` | 3.9.1 | app image `lib/` | Apache-2.0 |
| `org.apache.flink:flink-sql-connector-kafka` | 3.3.0-1.20, SHA-256 `1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d` (checked by `ADD --checksum`) | Flink image `/opt/flink/lib` (JobManager, TaskManager, SQL Gateway) | Apache-2.0 |
| `io.opentelemetry.javaagent:opentelemetry-javaagent` | 2.34.0, SHA-256 `da44154cea128257779c8a782b9651fd7c0c6aa177cd3810f9a867ac8509555d` (checked by `ADD --checksum`) | App image and Flink image `/opt/otel/opentelemetry-javaagent.jar` | Apache-2.0 |
| `com.fasterxml.jackson.core:*` | 2.17.2 | both | Apache-2.0 |
| `org.slf4j:slf4j-api`, `slf4j-simple` | 2.0.16 | app image | MIT |
| `org.junit.jupiter:*` | 5.10.3 | tests only | EPL-2.0 |

Flink, the Flink Table API, the Kafka SQL connector and the Fluss connector are on the Flink
image's classpath and are not bundled into the job jar.

## Python packages (downloaded from PyPI during the python-runner build)

| Package | Version | License |
| --- | --- | --- |
| `pyfluss` (Apache Fluss Python client) | 1.0.0 | Apache-2.0 |
| `pyarrow` | 26.0.1 | Apache-2.0 |
| `pandas` | 3.1.1 | BSD-3-Clause |
| `numpy` | 2.5.4 | BSD-3-Clause |
| `python-dateutil` | 4.0.0 | Apache-2.0 / BSD |
| `tzdata` | 2026.6 | Apache-2.0 |
