# Social Trend Detector

Near real-time hashtag/topic trend analyzer for social media events, built with **Apache Beam**, **Apache Kafka**, and **Streamlit**.

> **Note:** This project uses a synthetic event generator instead of a real social media API connection. The producer simulates user interactions (posts, comments, likes, shares) across tech-related topics with weighted probability distributions. See [Extending to Real Data Sources](#extending-to-real-data-sources) for how to connect it to real platforms.

---

## Architecture

```
generator/              pipeline/                  dashboard/
┌──────────────┐       ┌──────────────────┐       ┌──────────────┐
│  producer.py │──────▶│  beam_pipeline.py │──────▶│ dashboard.py │
│  Fake social │ Kafka │  Apache Beam      │ JSON  │  Streamlit   │
│  events      │       │  DirectRunner     │       │  Live charts │
└──────────────┘       └──────────────────┘       └──────────────┘
        │                       │
        ▼                       ▼
┌──────────────────────────────────────┐
│        Docker Compose                │
│  Zookeeper ─▶ Kafka ─▶ Kafka UI     │
└──────────────────────────────────────┘
```

## How It Works

### Data Flow

The producer generates JSON events that look like this:

```json
{
  "timestamp": "2025-06-15 14:32:07",
  "user_id": "user_42",
  "action": "post",
  "topic": "AI",
  "value": 1
}
```

Each event represents a simulated user interaction on a social platform. Topics are selected using weighted random sampling — `AI` appears ~30% of the time, while others like `kafka` or `docker` appear ~10% each — mimicking how real trends have unequal popularity.

Events flow through Kafka into the Apache Beam pipeline, which consumes them in micro-batches, aggregates counts per topic, and writes results to a JSON file. Streamlit reads that file every 3 seconds and renders live charts.

### The Stack

**Apache Kafka** acts as the message broker between the producer and the pipeline. The Docker Compose setup runs Kafka with dual listeners — an internal one (`kafka:29092`) for container-to-container traffic and an external one (`localhost:9092`) for the host machine. This is a common pattern when running Kafka in Docker on development environments.

**Apache Zookeeper** manages Kafka's cluster metadata, broker registration, and topic configuration. Kafka depends on it for leader election and partition assignment. While newer Kafka versions support KRaft mode (removing the Zookeeper dependency), this project uses the traditional setup since Confluent's Docker images have more mature support for it.

**Apache Beam** is the core processing engine. The pipeline uses Beam's `DirectRunner` to execute transforms locally without needing a distributed cluster. The key transforms are:
- `beam.Create` — ingests the micro-batch of messages
- `beam.Map` — extracts key-value pairs `(topic, 1)` from each event
- `beam.CombinePerKey(sum)` — aggregates counts per topic within the batch
- `beam.io.WriteToText` — persists results to disk

Beam's programming model is portable — the same pipeline code can run on Flink, Spark, or Dataflow by changing the runner. This project uses `DirectRunner` for simplicity, but the pipeline logic itself is runner-agnostic.

**Confluent Kafka (Python client)** handles the consumer side. The pipeline uses `confluent_kafka.Consumer` to poll messages in configurable time windows, giving control over batch size and polling timeout.

**Kafka UI** provides a web interface at `localhost:8080` to inspect topics, partitions, messages, and consumer groups. Useful for debugging and verifying that events are flowing correctly.

**Streamlit** renders the dashboard with auto-refresh. It reads the accumulated JSON output and displays KPI metrics, a bar chart of topic popularity, a time series of trend evolution, and a data table of recent processing windows.

## Project Structure

```
trend-detector/
├── docker-compose.yml          # Kafka + Zookeeper + Kafka UI
├── generator/
│   └── producer.py             # Synthetic event generator → Kafka
├── pipeline/
│   └── beam_pipeline.py        # Apache Beam processing pipeline
├── dashboard.py                # Streamlit visualization
├── requirements.txt
├── output/                     # Auto-created by pipeline
│   └── trend_data.json
└── README.md
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- WSL2 if running on Windows

### 1. Start the infrastructure

```bash
docker compose up -d
```

Wait until Kafka is healthy:

```bash
docker compose ps
# kafka should show status "healthy"
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the producer (Terminal 1)

```bash
python generator/producer.py
```

You should see events being sent every 0.5 seconds.

### 4. Start the Beam pipeline (Terminal 2)

```bash
python pipeline/beam_pipeline.py
```

The pipeline will print aggregated counts per topic each cycle.

### 5. Launch the dashboard (Terminal 3)

```bash
streamlit run dashboard.py
```

Open `http://localhost:8501` in your browser to see the live dashboard.

### Optional: Kafka UI

Open `http://localhost:8080` to inspect topics and messages in the broker.

## Beam Concepts Demonstrated

| Concept | Where | What it does |
|---|---|---|
| `beam.Create` | Pipeline ingestion | Creates a PCollection from an in-memory list of Kafka messages |
| `beam.Map` | Transform | Extracts `(topic, 1)` key-value pairs from each event |
| `beam.CombinePerKey` | Aggregation | Sums values per key, producing `(topic, total_count)` |
| `beam.ParDo` / `DoFn` | Custom transform | Formats aggregated results into JSON structures |
| `beam.io.WriteToText` | Sink | Writes pipeline output to a file (JSONL format) |
| `PipelineOptions` | Configuration | Configures the runner, workers, and execution mode |
| `DirectRunner` | Execution | Runs the pipeline locally without a distributed cluster |

## Extending to Real Data Sources

The producer is intentionally decoupled from the pipeline — it just pushes JSON events to a Kafka topic. To connect a real data source, you only need to replace `generator/producer.py` with one that reads from an actual API:

**Twitter/X API (via Tweepy):** Stream tweets by keyword and push them to the same `social-events` topic. Map each tweet to the existing event schema (`user_id`, `action: "post"`, `topic` extracted from hashtags).

**Reddit API (via PRAW):** Poll subreddits for new posts and comments. Each submission becomes an event with `topic` set to the subreddit name.

**Mastodon Streaming API:** Connect to the public timeline via WebSocket and extract hashtags as topics.

**Webhooks:** If your platform supports webhooks (GitHub, Discord, Slack), expose a small Flask/FastAPI endpoint that receives events and forwards them to Kafka.

The pipeline and dashboard require zero changes — they consume from the same Kafka topic regardless of the source.

## Scaling Beyond DirectRunner

The pipeline uses `DirectRunner` for local development. To scale to production workloads:

- **Apache Flink**: Change to `--runner=FlinkRunner` and point to a Flink cluster. Requires a Flink job server and SDK harness containers.
- **Google Cloud Dataflow**: Use `--runner=DataflowRunner` with GCP credentials. Beam handles autoscaling and resource management.
- **Apache Spark**: Use `--runner=SparkRunner` with a Spark cluster. Good if you already have Spark infrastructure.

The pipeline code stays the same — only the runner configuration changes. This is the core value proposition of Apache Beam's portability layer.

## License

MIT
