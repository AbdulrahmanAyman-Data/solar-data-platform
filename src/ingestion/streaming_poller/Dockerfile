FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

# Keep the image limited to its declared runtime dependencies. In particular,
# do not copy ignored local source files into an image built from the skeleton.
# When the team adds poller.py to Git, add an explicit COPY for it here.
CMD ["sh", "-c", "if [ -f /app/poller.py ]; then exec python /app/poller.py; else echo 'poller.py is not included yet; container is idle.'; exec tail -f /dev/null; fi"]
