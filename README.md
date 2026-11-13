# Valkyrie

[![Paper](https://img.shields.io/badge/Paper-alphaXiv-B31B1B)](https://www.alphaxiv.org/abs/valkyrie) [![valkyrie-cli on PyPI](https://img.shields.io/pypi/v/valkyrie-cli?label=valkyrie-cli)](https://pypi.org/project/valkyrie-cli/) [![valkyrie-sdk on PyPI](https://img.shields.io/pypi/v/valkyrie-sdk?label=valkyrie-sdk)](https://pypi.org/project/valkyrie-sdk/) ![CLI coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/vals-ai/Valkyrie/gh-pages/cli-coverage.json) ![Tracker coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/vals-ai/Valkyrie/gh-pages/tracker-coverage.json)

Valkyrie orchestrates scalable, reproducible evaluations for AI agents. Run it on infrastructure that Vals AI hosts, or on your own machine with a local Tracker.

Guides, the CLI reference and the Python SDK reference are published at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie).

<p align="center">
  <img src="docs/images/valk-tui.gif" alt="valkyrie tui showing a run's tasks, errors first, next to the highlighted task's log" width="720">
</p>

## Quickstart

Run Claude Code on one Terminal-Bench task with Vals AI's hosted Valkyrie. You need [uv](https://docs.astral.sh/uv/), a Vals AI account (email [contact@vals.ai](mailto:contact@vals.ai)), a [Modal](https://modal.com) account, and an Anthropic API key.

1. Install the CLI. It installs as `valkyrie`, with the short alias `valk`:

   ```bash
   uv tool install valkyrie-cli
   ```

   If you installed the CLI from GitHub before, remove that install first with `uv tool uninstall valkyrie`.

2. Sign in:

   ```bash
   valkyrie login
   ```

3. Connect Modal by following [Set up Modal](https://docs.valkyrie.vals.ai/get-started/sandbox-providers#set-up-modal). It's set once per organization, so skip it if someone in yours already did.

4. Install Claude Code from the [public agent registry](https://github.com/vals-ai/public-agent-registry), and store your Anthropic API key at the prompt:

   ```bash
   valkyrie agent install \
     https://github.com/vals-ai/public-agent-registry/tree/main/examples/claude_code-v1.0.0

   valkyrie secret set anthropic-api-key
   ```

5. Install Terminal-Bench 2 from Harbor as the dataset `tb/tb2`:

   ```bash
   valkyrie dataset install tb/tb2 terminal-bench/terminal-bench-2 --location harbor
   ```

6. Run one task:

   ```bash
   valkyrie run start --agent claude_code-v1.0.0 --dataset tb/tb2 \
     --model anthropic/claude-sonnet-4-6 --provider modal \
     -s ANTHROPIC_API_KEY anthropic-api-key --slice 0:1 --concurrency 1
   ```

7. Watch it with `valkyrie tui`.

The [quickstart guide](https://docs.valkyrie.vals.ai/get-started/quickstart) walks through each step and how to read the result.

## Browse runs in the terminal

`valkyrie tui` lists your organization's runs with live status, so everyone signed in to the same organization sees the same runs. Open one to see its tasks, errors first, next to the highlighted task's log.

- `/` filters the list, `e` groups a run's errors by message, and `s` picks how far back the list goes.
- `y` copies the matching `valkyrie` command, to run yourself or hand to an agent.
- `valkyrie tui --fake` opens it on sample runs, with no account needed.

## Where to go next

| Topic | Location |
| --- | --- |
| Packaging an agent and writing a contract | [agents/package-an-agent](https://docs.valkyrie.vals.ai/agents/package-an-agent) |
| Starting, monitoring, and managing runs | [runs/start](https://docs.valkyrie.vals.ai/runs/start) |
| Results and output artifacts | [runs/results-and-outputs](https://docs.valkyrie.vals.ai/runs/results-and-outputs) |
| Publishing your own tasks as a dataset | [benchmarks/datasets](https://docs.valkyrie.vals.ai/benchmarks/datasets) |
| Every CLI command | [reference/cli/index](https://docs.valkyrie.vals.ai/reference/cli/index) |
| Python SDK | [sdk/quickstart](https://docs.valkyrie.vals.ai/sdk/quickstart) |
| Running Tracker locally without AWS | [self-hosting/local-tracker](https://docs.valkyrie.vals.ai/self-hosting/local-tracker) |
| Contributing to Valkyrie | [CONTRIBUTING.md](CONTRIBUTING.md) |

## Citation

If you use Valkyrie in your research, cite [the paper](https://doi.org/10.1145/3786335.3813231):

```bibtex
@inproceedings{forzano2026valkyrie,
  author    = {Forzano, Jarett and Almatov, Omar and Nashold, Langston and Ravi, Nikil and Kassian, Orestes},
  title     = {Valkyrie: A Microservice-Based Framework for Scalable Evaluation of AI Agents},
  booktitle = {Proceedings of the 1st ACM Conference on Agentic and AI Systems},
  series    = {CAIS '26},
  year      = {2026},
  month     = {may},
  address   = {San Jose, CA, USA},
  publisher = {ACM},
  location  = {New York, NY, USA},
  numpages  = {5},
  doi       = {10.1145/3786335.3813231},
  url       = {https://doi.org/10.1145/3786335.3813231}
}
```
