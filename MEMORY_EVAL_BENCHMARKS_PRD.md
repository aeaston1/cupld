# PRD: LLM Memory System Eval Benchmarks

Status: Draft
Last updated: 2026-05-08

## Summary

This PRD defines the benchmark program for evaluating an LLM memory system built on or integrated with `cupld`. The subject under test is not just a database, retriever, or markdown parser. It is the full memory loop an LLM agent depends on:

```text
signal -> ingest -> normalize -> retrieve -> answer -> cite -> write back -> maintain -> replay
```

The benchmark design should take direct inspiration from Garry Tan's [`gbrain`](https://github.com/garrytan/gbrain) and [`gbrain-evals`](https://github.com/garrytan/gbrain-evals) repositories, while keeping the initial required scope narrow:

- Markdown or local files are the human-readable source of truth.
- The derived index can be rebuilt and compared across adapters.
- Memory pages separate current synthesized state from append-only evidence history.
- Retrieval is evaluated with sealed qrels, ablations, and reproducible scorecards.
- Real opt-in memory queries can be captured and replayed as regression tests.
- Benchmarks test the agent-memory workflow, not only top-k vector recall.

Initial required benchmark coverage should be:

- A custom BrainBench-style fictional corpus owned by `cupld`.
- A custom real-query replay harness for dogfood regressions.
- One public benchmark: LongMemEval.

Other public benchmarks are optional future extensions, not launch requirements.

The result should be a suite that can answer:

1. Does the memory system recall the right information at the right time?
2. Does it preserve provenance, timelines, contradictions, and updates instead of flattening them away?
3. Does it improve downstream LLM answers and actions without causing hallucinated or stale responses?
4. Which memory architecture choices actually matter: keyword, vector, graph, query expansion, summaries, timelines, citations, and maintenance jobs?
5. What are the quality, latency, cost, and privacy tradeoffs at realistic scale?

## Product Scope

### System Under Test

Each eval run should identify the memory stack as an adapter with a fixed configuration. A `cupld` adapter may be one implementation among several baselines.

The memory stack includes:

- Source storage: markdown notes, chat transcripts, email-like messages, meeting notes, calendar events, voice transcripts, PDFs, web pages, or synthetic fixtures.
- Normalization: entity extraction, deduplication, link extraction, timeline extraction, claim extraction, citation extraction, and source trust tagging.
- Memory representation: source documents, compiled truth, append-only timeline, raw provenance, graph edges, embeddings, indexes, and optional summaries.
- Retrieval: keyword, vector, graph traversal, hybrid fusion, reranking, query expansion, temporal-aware ranking, source-aware ranking, and deduplication.
- Answering: reader model prompt construction, citation use, abstention, contradiction handling, and final answer correctness.
- Write-back: when the agent updates memory after a new conversation, correction, or external signal.
- Maintenance: citation repair, stale page cleanup, orphan detection, backlink enforcement, re-embedding, compaction, and replay of captured regressions.
- Interfaces: CLI, MCP, API, scheduled jobs, and any tool bridge used by an LLM agent.

### Non-Goals

- A generic LLM leaderboard.
- A benchmark only for `cupld` storage internals.
- A single-number claim that hides retrieval, answer, provenance, and cost tradeoffs.
- A suite that requires private user data, hosted vector databases, or production credentials for the core CI path.

## GBrain/GBrain-Evals Design Principles To Borrow

### Memory Model

GBrain's most useful product pattern is the distinction between:

- `compiled_truth`: the current best answer or synthesized state for a person, company, concept, project, or decision.
- `timeline`: an append-only evidence trail of dated events, source observations, and updates.

The eval suite should test that an LLM memory system can update the current state without destroying historical evidence. A benchmark case should be able to ask both:

- "What is the user's current role?"
- "What did the user believe in March before the correction?"

### Adapter-First Evaluation

GBrain-evals treats each memory stack or configuration as an adapter that ingests the same raw pages and answers the same queries. The runner owns scoring, and adapters must not see gold labels.

This PRD adopts that pattern:

- `cupld_text_only`
- `cupld_graph`
- `cupld_hybrid`
- `cupld_hybrid_temporal`
- `vector_only`
- `keyword_only`
- `full_context`
- `oracle_retrieval`
- any third-party memory provider adapter added later

### Sealed Gold Data

The benchmark runner must enforce a hard boundary:

- Adapters receive only public input: messages, documents, metadata, timestamps, and allowed source content.
- Scorers receive gold data: evidence IDs, answer labels, entity IDs, contradiction labels, poisoning labels, and qrels.
- Reports include enough traces for debugging but not enough to let an adapter tune against hidden labels accidentally.

### Portable Schemas

GBrain-evals makes schema contracts part of the benchmark, not an implementation detail. The `cupld` memory eval harness should do the same with portable schemas for:

- Corpus manifests: corpus ID, generator metadata, item hashes, perturbation labels, license, and split.
- Public probes: the scrubbed query shape shown to adapters.
- Scorecards: config card, adapter version, corpus hash, metrics, N, cost, latency, and verdict.
- Tool schemas: read tools plus dry-run write tools for agent-behavior evals.
- Transcripts: model calls, tool calls, tool results, and final answers.
- Evidence contracts: structured judge input that hides raw tool traces when a judge is used.

Schema validation should run in CI. A benchmark is not accepted unless its bundle validates.

### Real-Query Replay

GBrain's BrainBench-Real pattern captures real opt-in `query` and `search` calls, scrubs them, exports NDJSON, and replays them against a changed retrieval stack using:

- mean Jaccard@k
- top-1 stability
- latency delta
- top regression examples

`cupld` should support the same style once there is enough real usage. This is not a substitute for qrels, but it is a strong regression alarm for memory-system changes.

### Published Scorecards

Every benchmark run worth keeping should produce a standalone scorecard:

- What was tested.
- Which adapters ran.
- Which exact versions and commits ran.
- What the benchmark measures and does not measure.
- Headline results.
- Per-category breakdown.
- Latency and cost.
- Reproduction commands.
- Methodology and caveats.
- Links to source data, schemas, and raw artifacts.

Reports should publish null results and losses, not only wins. If graph retrieval helps relational queries but does nothing for conceptual recall, the report should say that plainly. If query expansion adds cost with no lift on a public benchmark, that is still a useful result.

For nondeterministic paths, publish tiers should use repeated runs or tolerance bands:

- `N=1` smoke for local iteration.
- `N=5` development baseline.
- `N=10` publishable scorecard where model or judge variance matters.

## Evaluation Architecture

### Runner Contract

Each benchmark runner should produce and consume a portable bundle:

```text
eval_bundle/
  manifest.toml
  corpus/
    pages/
    messages/
    attachments/
  queries.jsonl
  expected.jsonl
  graph_setup.cupld
  licenses/
  README.md
```

`manifest.toml`:

```toml
id = "brainbench-fictional-life-v1"
benchmark = "BrainBench-style custom"
adapter_version = "0.1.0"
source_revision = "..."
license = "MIT"
generated_at = "2026-05-08T00:00:00Z"
random_seed = 42
pii = "none; fictional corpus"
```

`queries.jsonl`:

```json
{"case_id":"q-001","question":"Who introduced Amara to the founder at Helio Labs?","expected_output_type":"canonical_entity_id","tags":["identity","relational"],"top_k":[1,5,10]}
```

`expected.jsonl`:

```json
{"case_id":"q-001","gold_answers":["people/maya-chen"],"gold_evidence":[{"source_id":"meetings/2026-04-12-partner-sync.md","span_id":"timeline-003"}],"unanswerable":false}
```

### Adapter Interface

The eventual implementation should expose a simple adapter contract:

```ts
interface MemoryAdapter {
  name: string;
  init(corpus: PublicCorpus, config: AdapterConfig): Promise<MemoryState>;
  ingest(signal: PublicSignal, state: MemoryState): Promise<void>;
  retrieve(query: PublicQuery, state: MemoryState): Promise<RankedMemory[]>;
  answer?(query: PublicQuery, ranked: RankedMemory[], state: MemoryState): Promise<Answer>;
  maintain?(state: MemoryState): Promise<MaintenanceReport>;
  teardown?(state: MemoryState): Promise<void>;
}
```

The runner should never inspect adapter internals. It only scores outputs.

### Tool Bridge And Dry-Run Writes

Agent-memory benchmarks need to score more than retrieval. They must also verify whether the agent would write the right memory, citation, backlink, task update, or correction.

The harness should expose:

- Read-only tools for lookup, page fetch, graph traversal, timeline fetch, citation fetch, and source inspection.
- Dry-run write tools for page update, link creation, memory correction, citation repair, and task write-back.
- A transcript recorder that logs tool calls, tool results, model calls, final answers, and dry-run write intents.

Dry-run writes let Cats 8 and 9 style workflow tests score behavior without mutating fixture state.

### Core Output Types

Each retrieval result should include enough information to score evidence and provenance:

```json
{
  "memory_id": "people/amara-okafor",
  "source_id": "meetings/2026-04-12-partner-sync.md",
  "span_id": "timeline-003",
  "rank": 1,
  "score": 0.91,
  "snippet": "2026-04-12: Maya introduced Amara to Helio Labs...",
  "memory_layer": "timeline",
  "source_trust": "trusted",
  "timestamp": "2026-04-12T15:30:00Z"
}
```

## Benchmark Portfolio

Required launch scope is deliberately small: custom evals first, then LongMemEval as the one external public benchmark. Additional public suites should only be added after the custom corpus, adapters, scorecard format, and LongMemEval runner are stable.

### P0: Custom BrainBench-Style Corpus

Priority: Required before public benchmark adapters become meaningful.

Purpose: Create a redistributable fictional memory corpus that tests agent memory behavior under controlled conditions, similar in spirit to `gbrain-evals`' fictional-life corpora.

Recommended corpus:

- 80 people
- 80 companies
- 50 meetings
- 50 email threads
- 300 chat or Slack-like messages
- 20 calendar events
- 40 first-person notes
- 30 concept pages
- 10 long documents or PDFs converted to text
- planted contradictions, stale facts, aliases, source-swamp distractors, prompt-injection snippets, implicit preferences, and missing evidence cases

This corpus should be fully fictional and redistributable.

Required categories:

| Cat | Name | What It Tests | Primary Metrics |
| --- | --- | --- | --- |
| 1 | Direct Recall | Single-memory lookup over clean source content | Hit@k, answer EM/F1 |
| 2 | Semantic Recall | Paraphrased questions with little keyword overlap | Recall@k, MRR |
| 3 | Identity Resolution | Aliases, emails, handles, misspellings, duplicate entities | entity accuracy, merge/split error rate |
| 4 | Relationship Graph | Typed links such as works_at, founded, invested_in, attended, advises | edge precision/recall, graph-answer accuracy |
| 5 | Temporal Memory | as-of, before/after, first/latest, date-range queries | temporal accuracy, stale-answer rate |
| 6 | Knowledge Updates | current truth changes while old evidence remains queryable | latest-state accuracy, historical-state accuracy |
| 7 | Compiled Truth vs Timeline | current summary should rank above timeline noise when appropriate | layer accuracy, evidence coverage |
| 8 | Provenance | answer claims must cite source pages and spans | citation accuracy, unsupported-answer rate |
| 9 | Contradictions | conflicting sources, corrections, revoked facts | contradiction classification, resolution accuracy |
| 10 | Abstention | no memory, ambiguous memory, insufficient evidence | abstention precision/recall |
| 11 | Source Swamp | important curated memories mixed with bulk transcripts and low-signal sources | top-1 accuracy, source-boost effectiveness |
| 12 | Prompt Injection | malicious text inside memory must not override system behavior | poison resistance, unsafe-compliance rate |
| 13 | Workflow Compliance | agent follows brain-first lookup, citation, write-back, and tiering rules | rubric pass rate |
| 14 | Maintenance | orphan links, stale citations, missing backlinks, stale embeddings | repair accuracy, no-data-loss rate |
| 15 | Interface Contract | CLI/MCP/API parity, validation, trust boundaries | parity pass rate, silent-corruption count |
| 16 | Performance | quality and latency at 1K, 10K, and 100K memory items | p50/p95/p99, cost, DB size |

Acceptance:

- P0 custom corpus must run without network access or model APIs in retrieval-only mode.
- All fixtures must be reviewable in git.
- Gold labels must be sealed from adapters.

### P0: Real-Query Replay

Priority: Required once there is dogfood usage.

Purpose: Catch retrieval regressions from real memory usage without exposing private data by default.

Capture must be opt-in and privacy-positive:

- Off by default.
- Explicit env var or config flag.
- PII scrubber for emails, phone numbers, tokens, keys, and obvious secrets.
- Export to NDJSON.
- Retention controls.

Replay metrics:

- mean Jaccard@k
- top-1 stability
- mean latency delta
- rows over 2x latency
- rows errored or skipped
- top regressions by query

Replay is not a gold-label benchmark. It answers: "Did this change move retrieval behavior for real queries?"

### P1: LongMemEval

Sources: [LongMemEval ICLR 2025 page](https://proceedings.iclr.cc/paper_files/paper/2025/hash/d813d324dbf0598bbdc9c8e79740ed01-Abstract-Conference.html), [LongMemEval GitHub](https://github.com/xiaowu0162/LongMemEval), [GBrain LongMemEval report](https://github.com/garrytan/gbrain-evals/blob/main/docs/benchmarks/2026-05-07-longmemeval-s.md)

Priority: Required public benchmark for the initial external benchmark track.

Why: LongMemEval is directly aligned with long-term chat assistant memory. It tests information extraction, multi-session reasoning, temporal reasoning, knowledge updates, and abstention.

Modes:

- Retrieval-only: whether answer-bearing sessions appear in top-k.
- End-to-end QA: retrieved context passed to a pinned reader, scored by the public evaluator or a pinned judge.

Required splits:

- `_oracle`: sanity and debugging.
- `_s`: standard reporting split.

Optional split:

- `_m`: harder noise split once runtime and cost are acceptable.

Metrics:

- R@5 as a headline to match published memory-system reports.
- Recall@1/5/10/20.
- MRR.
- Per-question-type recall.
- QA accuracy or judged correctness.
- Temporal-reasoning delta.
- Cost per 1000 questions.

Required adapters:

- keyword-only
- vector-only
- hybrid
- hybrid + temporal signal
- hybrid + query expansion
- full-context reader where feasible
- oracle retrieval upper bound

### Optional Future Public Benchmarks

These benchmarks are useful candidates after the required custom suite and LongMemEval runner are stable. They are not required for the initial launch scorecard.

| Benchmark | Priority | Why Add It Later | Primary Metrics |
| --- | --- | --- | --- |
| [ConvoMem](https://huggingface.co/datasets/Salesforce/ConvoMem) | Optional P2 | Large conversational-memory coverage across user facts, assistant facts, changing evidence, abstention, preferences, implicit connections, and context-size sensitivity | accuracy by evidence category, Recall@k by context size, latest-state accuracy, abstention precision/recall |
| [LoCoMo](https://snap-research.github.io/locomo/) | Optional P2 | Long-horizon multi-session dialogue, temporal dynamics, causal event reasoning, and multimodal conversation | QA F1/judged correctness, Evidence Recall@k, temporal-order accuracy, hallucination rate |
| [MemoryBench](https://github.com/LittleDinoC/MemoryBench/) | Optional P3 | Continual learning from accumulated user feedback | improvement after feedback, forgetting rate, feedback ingestion latency |
| [MemBench](https://aclanthology.org/2025.findings-acl.989/) | Optional P3 | Factual versus reflective memory, observation versus participation scenarios | factual accuracy, reflective accuracy, capacity by memory length |
| [MemoryArena](https://memoryarena.github.io/) | Optional P3 | Agentic memory use across multi-session subtasks | cross-session task success, dependency success rate, memory-use attribution |
| [STaRK](https://stark.stanford.edu/) | Optional stress test | Semi-structured graph plus text retrieval | retrieval Recall@k, graph-text join accuracy |
| [BEIR](https://github.com/beir-cellar/beir) | Optional stress test | Broad text retrieval sanity baseline | nDCG@k, Recall@k |
| [RULER](https://github.com/NVIDIA/RULER) | Optional stress test | Synthetic long-context failure modes | task accuracy by context length |
| [NoLiMa](https://proceedings.mlr.press/v267/modarressi25a.html) | Optional stress test | Nonliteral long-context retrieval | nonliteral retrieval accuracy |
| [CRAG](https://github.com/facebookresearch/CRAG) | Optional stress test | Dynamic factual QA and grounding | grounded answer accuracy, hallucination rate |

Optional additions must not block the first scorecard. Each optional benchmark needs a short adoption note before implementation that covers license, data size, runner cost, whether redistribution is allowed, expected CI tier, and which gap it fills beyond custom evals plus LongMemEval.

## Required Ablations

Every benchmark scorecard should include ablations that isolate system choices:

| Adapter | Purpose |
| --- | --- |
| `no_memory` | Reader model without external memory |
| `full_context` | Reader receives all context where feasible |
| `keyword_only` | Sparse retrieval baseline |
| `vector_only` | Commodity RAG baseline |
| `hybrid` | Keyword + vector fusion |
| `hybrid_no_graph` | Measures graph contribution |
| `graph_only` | Measures typed relationship precision |
| `hybrid_temporal` | Measures timeline-aware ranking |
| `hybrid_no_compiled_truth_boost` | Measures compiled truth ranking lift |
| `hybrid_no_source_boost` | Measures source-swamp resistance |
| `hybrid_expansion` | Measures query expansion lift or null result |
| `oracle_retrieval` | Upper bound for reader-model answer quality |

Scorecards should say when an ablation produces a null result. A null result is useful product information.

## Metrics

### Retrieval Metrics

- Precision@k.
- Recall@k.
- Hit@k.
- MRR.
- nDCG@k.
- Mean reciprocal evidence rank.
- Evidence precision@k.
- Duplicate rate.
- Source-layer accuracy: compiled truth, timeline, raw source, summary, attachment.
- Stale-evidence rate.
- Poisoned-evidence exposure rate.

### Answer Metrics

- Exact match.
- Token F1.
- Multiple-choice accuracy.
- Judged correctness with pinned judge.
- Supported-answer rate.
- Citation accuracy.
- Hallucination rate.
- Abstention precision/recall.
- Contradiction handling accuracy.
- Temporal correctness.

### Memory Lifecycle Metrics

- Ingestion fidelity.
- Entity extraction precision/recall.
- Entity merge/split error rate.
- Typed edge precision/recall.
- Timeline extraction precision/recall.
- Compiled truth update accuracy.
- Historical evidence preservation.
- Write-back correctness.
- Maintenance repair accuracy.
- Idempotency on repeated sync/import.

### Operational Metrics

- Initial import throughput.
- Incremental import latency.
- Query p50/p95/p99.
- End-to-end answer latency.
- Cost per 1000 queries.
- Storage bytes per memory item.
- Embedding cache hit rate.
- Index rebuild time.
- Memory/tool error rate.
- Interface parity pass rate.

## Custom Eval Framework

Custom evals are first-class. Public benchmarks will not cover product-specific memory semantics.

### Layout

```text
evals/
  custom/
    manifest.toml
    cases/
      compiled_truth_latest_state/
        case.yaml
        corpus/
          notes/
          messages/
      prompt_injection_memory/
        case.yaml
        corpus/
```

### Case Schema

```yaml
id: compiled_truth_latest_state
version: 1
title: Current truth updates while older timeline evidence remains queryable
tags:
  - compiled_truth
  - timeline
  - knowledge_update
  - temporal
setup:
  adapter_modes:
    - cupld_hybrid
    - vector_only
  corpus_root: corpus
steps:
  - kind: ingest
    signal: messages/session-001.json
  - kind: retrieve
    name: initial_current_role
    question: "What is Amara's current role?"
    top_k: 5
  - kind: ingest
    signal: messages/session-002-correction.json
  - kind: retrieve
    name: updated_current_role
    question: "What is Amara's current role now?"
    top_k: 5
  - kind: retrieve
    name: historical_role
    question: "What role did Amara have before the correction?"
    top_k: 5
assertions:
  retrieval:
    - name: updated_current_role
      gold_evidence:
        - source_id: messages/session-002-correction.json
      stale_evidence:
        - source_id: messages/session-001.json
  answer:
    - name: updated_current_role
      gold_answers:
        - "Head of Platform"
    - name: historical_role
      gold_answers:
        - "VP of Product"
metrics:
  required:
    - recall_at_5
    - stale_evidence_rate
    - exact_match
```

### Required Hand-Written Case Families

- Current truth versus timeline history.
- User correction supersedes stale assistant memory.
- Contradictory sources with source-trust precedence.
- Exact alias versus ambiguous alias collision.
- Relationship query requiring typed graph edges.
- Temporal query requiring "first", "latest", "before", "after", and "as of".
- Source swamp: curated note hidden among bulk transcripts.
- Prompt injection inside retrieved memory.
- Abstention when memory is absent.
- Agent writes a new memory and later uses it.
- Citation required for every answer claim.
- Interface parity across CLI and MCP.

## CI And Release Gates

### PR Gate

Run:

- P0 custom smoke subset.
- Adapter contract tests.
- Sealed-gold leakage tests.
- Retrieval-only tests with no network and no model API.
- Markdown/source ingestion fidelity.
- CLI/MCP schema compatibility.

Budget:

- Under 5 minutes.
- No network.
- No external LLM calls.

### Nightly Gate

Run:

- Full P0 custom suite.
- BrainBench-style fictional corpus.
- LongMemEval `_oracle` and sampled `_s`.
- Real-query replay if opt-in fixture exists.
- Key ablations: keyword, vector, hybrid, graph, temporal.

Budget:

- Local or CI runner.
- Network only for configured embedding or reader model paths.
- Compare against `main` and previous release.

### Release Candidate Gate

Run:

- Full P0 custom suite.
- Full LongMemEval `_s`.
- Optional public benchmarks only if already adopted for that release.
- Performance at multiple corpus sizes.
- Maintenance and write-back workflows.
- Published scorecard.

## Initial Thresholds

Use relative thresholds until enough history exists:

- P0 custom deterministic pass rate: 100%.
- Sealed-gold leakage tests: 100%.
- LongMemEval `_s` R@5: no regression greater than 2 percentage points versus `main`.
- Real-query replay mean Jaccard@k: at least 0.85 for neutral retrieval changes.
- Real-query replay top-1 stability: at least 0.85 for tuning-only changes.
- Query latency p95: no regression greater than 20% on the same machine profile.
- Unsupported-answer rate: must not increase versus baseline.

After three release cycles, replace relative thresholds with absolute thresholds by benchmark and corpus size.

## Open Product Requirements For `cupld`

These requirements are implied by the eval plan:

- Query-aware context retrieval: a memory query should be accepted directly by the context command or API.
- Evidence-bearing context output: returned items need source IDs, span IDs, timestamps, and memory-layer labels.
- Ranking metadata: output should declare whether rank came from keyword, vector, graph, temporal, fusion, or reranker signals.
- Source-layer modeling: distinguish compiled truth, timeline, raw source, and generated summary.
- Temporal ranking: support latest/as-of/before/after retrieval signals.
- Adapter harness: expose enough CLI or API surface for benchmark adapters without depending on private Rust internals.
- Real-query capture: opt-in, scrubbed, exportable, replayable.
- Maintenance reports: machine-readable output for stale links, missing citations, orphan memories, and compaction.

## Risks

- Public benchmark scores can be metric-mismatched. Retrieval R@5, QA accuracy, and judged answer quality must never be mixed without labels.
- Memory systems can overfit small public benchmarks. Mitigate with custom corpora, hidden qrels, real-query replay, and optional public suites added over time.
- Synthetic corpora can be too clean. Add source swamp, contradictions, malformed inputs, and adversarial memory.
- Full-context baselines may beat retrieval at short context sizes. Report the crossover honestly.
- End-to-end QA can measure reader-model weakness instead of memory weakness. Always include retrieval-only and oracle-retrieval scores.
- Real-query capture has privacy risk. Keep it opt-in, scrubbed, local, and export-controlled.

## Source Index

Required and directly inspirational sources:

- GBrain: https://github.com/garrytan/gbrain
- GBrain eval docs: https://github.com/garrytan/gbrain/blob/master/docs/eval-bench.md
- GBrain eval capture schema: https://github.com/garrytan/gbrain/blob/master/docs/eval-capture.md
- GBrain-evals: https://github.com/garrytan/gbrain-evals
- GBrain-evals LongMemEval report: https://github.com/garrytan/gbrain-evals/blob/main/docs/benchmarks/2026-05-07-longmemeval-s.md
- LongMemEval: https://github.com/xiaowu0162/LongMemEval

Optional future benchmark sources:

- ConvoMem: https://huggingface.co/datasets/Salesforce/ConvoMem
- LoCoMo: https://snap-research.github.io/locomo/
- MemoryBench: https://github.com/LittleDinoC/MemoryBench/
- MemBench: https://aclanthology.org/2025.findings-acl.989/
- MemoryArena: https://memoryarena.github.io/
- STaRK: https://stark.stanford.edu/
- BEIR: https://github.com/beir-cellar/beir
- RULER: https://github.com/NVIDIA/RULER
- NoLiMa: https://proceedings.mlr.press/v267/modarressi25a.html
- CRAG: https://github.com/facebookresearch/CRAG
