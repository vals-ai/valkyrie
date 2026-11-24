# Valkyrie

[![Paper](https://img.shields.io/badge/Paper-alphaXiv-B31B1B)](https://www.alphaxiv.org/abs/valkyrie) [![valkyrie-cli on PyPI](https://img.shields.io/pypi/v/valkyrie-cli?label=valkyrie-cli)](https://pypi.org/project/valkyrie-cli/) [![valkyrie-sdk on PyPI](https://img.shields.io/pypi/v/valkyrie-sdk?label=valkyrie-sdk)](https://pypi.org/project/valkyrie-sdk/) ![CLI coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/vals-ai/Valkyrie/gh-pages/cli-coverage.json) ![Tracker coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/vals-ai/Valkyrie/gh-pages/tracker-coverage.json)

Valkyrie runs reproducible evaluations of AI agents at scale. Guides, the CLI reference and the Python SDK reference are at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie).

<p align="center">
  <img src="docs/images/valk-tui.gif" alt="valk tui showing a run's tasks, errors first, next to the highlighted task's log" width="720">
</p>

Use the same `valk` commands on hosted infrastructure or on your machine:

- **Hosted:** follow the [quickstart below](#quickstart). We run the Tracker and keep your results, so you can run many tasks at once in cloud sandboxes and your whole organization sees every run.
- **On your machine:** follow [Run Tracker locally](https://docs.valkyrie.vals.ai/self-hosting/local-tracker). You don't need an account.

## Quickstart

This runs Claude Code on one Terminal-Bench task in a Modal sandbox. You need [uv](https://docs.astral.sh/uv/), a [Modal](https://modal.com) account, an Anthropic API key, and a Vals AI account, which you can request at [contact@vals.ai](mailto:contact@vals.ai).

1. Install the CLI, sign in, and create an organization. Replace `my-team` with an unused ID.

   ```nushell
   uv tool install valkyrie-cli
   valk login
   valk org create my-team
   ```

2. [Connect Modal](https://docs.valkyrie.vals.ai/get-started/sandbox-providers#set-up-modal), then save your Anthropic key:

   ```nushell
   valk secret set ANTHROPIC_API_KEY
   ```

3. Add Claude Code and Terminal-Bench 2:

   ```nushell
   valk agent install https://github.com/vals-ai/public-agent-registry/tree/main/examples/claude_code-v1.0.0
   valk dataset install tb/tb2 terminal-bench/terminal-bench-2 --location harbor
   ```

4. Run one task and follow it to its score:

   ```bash
   valk run start --agent claude_code-v1.0.0 --dataset tb/tb2 \
     --model anthropic/claude-sonnet-4-6 --provider modal \
     -s ANTHROPIC_API_KEY ANTHROPIC_API_KEY --task-range 0:1 --follow
   ```

The [quickstart guide](https://docs.valkyrie.vals.ai/get-started/quickstart) explains each step, including how to join an existing organization.

## Browse runs in the terminal

`valk tui` lists your organization's runs with live status, so everyone signed in to the same organization sees the same runs. Open one to see its tasks, errors first, next to the highlighted task's log.

- `/` filters the list, `e` groups a run's errors by message, and `s` picks how far back the list goes.
- `y` copies the matching `valk` command, to run yourself or hand to an agent.

## Where to go next

| Topic | Location |
| --- | --- |
| Packaging an agent and writing a contract | [agents/package-an-agent](https://docs.valkyrie.vals.ai/agents/package-an-agent) |
| Starting, monitoring, and managing runs | [runs/start](https://docs.valkyrie.vals.ai/runs/start) |
| Results and output artifacts | [runs/results-and-outputs](https://docs.valkyrie.vals.ai/runs/results-and-outputs) |
| Publishing your own tasks as a dataset | [benchmarks/datasets](https://docs.valkyrie.vals.ai/benchmarks/datasets) |
| Building a benchmark service | [benchmarks/convert-a-benchmark](https://docs.valkyrie.vals.ai/benchmarks/convert-a-benchmark) |
| How the benchmark service framework works | [packages/create-benchmark-service](packages/create-benchmark-service/README.md) |
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
