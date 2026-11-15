# Isolated Python execution service for the Query Workspace (Python tab).
# Python 3.12 slim, pinned by digest, with the official Fluss Python client (pyfluss 1.0.0).
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

COPY requirements.txt /opt/runner/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r /opt/runner/requirements.txt \
 && python -c "import fluss; print('pyfluss import OK')"

COPY runner.py /opt/runner/runner.py
RUN useradd --uid 10002 --no-create-home --shell /usr/sbin/nologin runner

USER 10002
EXPOSE 8000
ENTRYPOINT ["python", "-I", "-B", "/opt/runner/runner.py"]
