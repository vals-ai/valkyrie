import io
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from unittest import mock
from uuid import UUID

import metric_process
import pytest
from botocore.exceptions import ClientError

from tracker.clients import metric_runner
from tracker.services.datasets import scoring
from tracker.services.datasets.ref import LocalDataset
from tracker.services.datasets.rewards import RewardMap
from tracker.clients.metric_runner import LambdaMetricInvoker, LocalMetricInvoker, MetricResult, MetricRunnerError
from tracker.services.datasets.scoring import MeanScorer, Score, ScriptScorer

ORG = UUID("00000000-0000-0000-0000-000000000001")
RESULTS = {
    "a": {"rewards": {"reward": 1.0}},
    "b": {"rewards": {"reward": 0.0}},
    "c": None,
}


class FakeInvoker:
    def __init__(self, metrics: dict[str, float] | None = None, error: str | None = None) -> None:
        self.metrics = {"reward": 0.25} if metrics is None else metrics
        self.error = error
        self.calls: list[tuple[str, str, list[RewardMap | None]]] = []

    async def invoke(self, tenant_id: str, script: str, rewards: Sequence[RewardMap | None]) -> MetricResult:
        self.calls.append((tenant_id, script, list(rewards)))
        if self.error is not None:
            raise MetricRunnerError(self.error)
        return MetricResult(metrics=dict(self.metrics), duration=0.5)


def script_loader(script: str = "print('metric')") -> Callable[[], Awaitable[str]]:
    async def load() -> str:
        return script

    return load


def scorer(invoker: FakeInvoker | None, org: UUID = ORG) -> ScriptScorer:
    return ScriptScorer(script_loader(), "sha256:script", org, invoker)


@pytest.mark.description("What a run's score is when the dataset has a metric script.")
class TestScriptScorer:
    @pytest.mark.description("The scripts reward metric times one hundred is the score and every metric is kept.")
    async def test_the_scripts_reward_metric_times_one_hundred_is_the_score_and_every_metric_is_kept(self) -> None:
        invoker = FakeInvoker({"reward": 0.25, "accuracy": 0.5})

        score = await scorer(invoker).score(RESULTS)

        assert score.final_score == 25.0
        assert score.properties["metrics"] == [{"reward": 0.25, "accuracy": 0.5}]
        assert score.properties["score_key"] == "reward" and score.properties["mean_reward"] == 0.25
        assert (score.properties["total"], score.properties["n_trials"], score.properties["n_errors"]) == (3, 2, 1)
        assert score.properties["scorer"] == {"digest": "sha256:script", "duration": 0.5}

    @pytest.mark.description(
        "The script gets the rewards in task order with none for a missing one and the org as tenant."
    )
    async def test_the_script_gets_the_rewards_in_task_order_with_none_for_a_missing_one_and_the_org_as_tenant(
        self,
    ) -> None:
        invoker = FakeInvoker()

        await scorer(invoker).score({"b": RESULTS["b"], "c": None, "a": RESULTS["a"]})

        assert invoker.calls == [(str(ORG), "print('metric')", [{"reward": 1.0}, {"reward": 0.0}, None])]

    @pytest.mark.description("Metrics with no single headline finish with every metric and no score.")
    async def test_metrics_with_no_single_headline_finish_with_every_metric_and_no_score(self) -> None:
        score = await scorer(FakeInvoker({"precision": 0.5, "recall": 0.75})).score(RESULTS)

        assert score.final_score is None
        assert score.properties["metrics"] == [{"precision": 0.5, "recall": 0.75}]
        assert score.properties["score_key"] is None

    @pytest.mark.description("A failing script leaves no score and never falls back to the mean.")
    async def test_a_failing_script_leaves_no_score_and_never_falls_back_to_the_mean(self) -> None:
        score = await scorer(FakeInvoker(error="The script exited with status 1: boom")).score(RESULTS)
        mean = await MeanScorer().score(RESULTS)

        assert score.final_score is None and mean.final_score is not None
        assert score.properties["scorer"] == {
            "digest": "sha256:script",
            "error": "The script exited with status 1: boom",
        }
        assert score.properties["metrics"] == []
        assert score.properties["reward_stats"] == {"reward": {1.0: ["a"], 0.0: ["b"]}}

    @pytest.mark.description("A tracker with no metric runner reports it instead of scoring by the mean.")
    async def test_a_tracker_with_no_metric_runner_reports_it_instead_of_scoring_by_the_mean(self) -> None:
        score = await scorer(None).score(RESULTS)

        assert score.final_score is None
        assert "No metric runner" in score.properties["scorer"]["error"]

    @pytest.mark.description("A script that cannot be read from storage leaves no score instead of failing the run.")
    async def test_a_script_that_cannot_be_read_from_storage_leaves_no_score_instead_of_failing_the_run(self) -> None:
        async def unreadable() -> str:
            raise MetricRunnerError("The metric script could not be read: gone")

        score = await ScriptScorer(unreadable, "sha256:script", ORG, FakeInvoker()).score(RESULTS)

        assert score.final_score is None
        assert score.properties["scorer"]["error"] == "The metric script could not be read: gone"

    @pytest.mark.description("A runner reply with no finite metrics is an error.")
    async def test_a_runner_reply_with_no_finite_metrics_is_an_error(self) -> None:
        score = await scorer(FakeInvoker({"reward": float("nan")})).score(RESULTS)

        assert score.final_score is None
        assert "no finite metrics" in score.properties["scorer"]["error"]


@pytest.mark.description("The request sent to the runner and the replies read from it.")
class TestLambdaMetricInvoker:
    def invoker(self, response: dict[str, Any]) -> tuple[LambdaMetricInvoker, mock.Mock]:
        client = mock.Mock()
        client.invoke.return_value = response
        with mock.patch.object(metric_runner.boto3, "client", return_value=client):
            return LambdaMetricInvoker("valkyrie-metric-runner-dev"), client

    @staticmethod
    def reply(body: dict[str, Any], **extra: Any) -> dict[str, Any]:
        return {"Payload": io.BytesIO(json.dumps(body).encode()), **extra}

    @pytest.mark.description("It invokes the function with the org as tenant and reads the metrics.")
    async def test_it_invokes_the_function_with_the_org_as_tenant_and_reads_the_metrics(self) -> None:
        invoker, client = self.invoker(self.reply({"metrics": {"reward": 0.5}, "duration": 1.5}))

        result = await invoker.invoke("org-1", "x = 1", [{"reward": 1.0}, None])

        assert result == MetricResult(metrics={"reward": 0.5}, duration=1.5)
        call = client.invoke.call_args.kwargs
        assert (call["FunctionName"], call["TenantId"]) == ("valkyrie-metric-runner-dev", "org-1")
        assert json.loads(call["Payload"]) == {"contract": 1, "script": "x = 1", "rewards": [{"reward": 1.0}, None]}

    @pytest.mark.description("A failed invoke is a runner error and not a crash.")
    async def test_a_failed_invoke_is_a_runner_error_and_not_a_crash(self) -> None:
        invoker, client = self.invoker({})
        client.invoke.side_effect = ClientError(
            {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}}, "Invoke"
        )

        with pytest.raises(MetricRunnerError, match="could not be invoked.*Rate exceeded"):
            await invoker.invoke("org-1", "x = 1", [])

    @pytest.mark.description("A reply that is not json is a runner error.")
    async def test_a_reply_that_is_not_json_is_a_runner_error(self) -> None:
        invoker, _client = self.invoker({"Payload": io.BytesIO(b"<html>")})

        with pytest.raises(MetricRunnerError, match="could not be invoked"):
            await invoker.invoke("org-1", "x = 1", [])

    @pytest.mark.description("An error from the handler or from lambda is a runner error.")
    @pytest.mark.parametrize(
        ("response", "message"),
        [
            pytest.param({"error": "The script ran longer than 300 seconds"}, "longer than 300", id="handler-error"),
            pytest.param({"errorMessage": "Task timed out after 900.00 seconds"}, "timed out", id="function-error"),
        ],
    )
    async def test_an_error_from_the_handler_or_from_lambda_is_a_runner_error(
        self, response: dict[str, Any], message: str
    ) -> None:
        invoker, _client = self.invoker(self.reply(response, FunctionError="Unhandled"))

        with pytest.raises(MetricRunnerError, match=message):
            await invoker.invoke("org-1", "x = 1", [])


HALVING_SCRIPT = (
    "import argparse, json\n"
    "parser = argparse.ArgumentParser()\n"
    "parser.add_argument('-i')\n"
    "parser.add_argument('-o')\n"
    "args = parser.parse_args()\n"
    "rewards = [row['reward'] for row in map(json.loads, open(args.i)) if row]\n"
    "json.dump({'reward': sum(rewards) / len(rewards) / 2, 'count': len(rewards)}, open(args.o, 'w'))\n"
)


@pytest.mark.description("A script run with uv on this machine.")
class TestLocalMetricInvoker:
    @pytest.mark.description("It runs the script on the rewards and reads its metrics.")
    async def test_it_runs_the_script_on_the_rewards_and_reads_its_metrics(self) -> None:
        result = await LocalMetricInvoker().invoke("org-1", HALVING_SCRIPT, [{"reward": 1.0}, {"reward": 0.5}, None])

        assert result.metrics == {"reward": 0.375, "count": 2.0}
        assert result.duration > 0

    @pytest.mark.description("A script that fails or writes bad output is a runner error.")
    @pytest.mark.parametrize(
        ("script", "message"),
        [
            pytest.param("import sys\nsys.exit('boom')\n", "exited with status 1: boom", id="exit-status"),
            pytest.param("x = 1\n", "wrote no output file", id="no-output"),
            pytest.param(
                "import json\njson.dump([1], open('output.json', 'w'))\n", "non-empty JSON object", id="not-an-object"
            ),
            pytest.param(
                "import json\njson.dump({'reward': 'high'}, open('output.json', 'w'))\n",
                "not a finite number",
                id="not-a-number",
            ),
        ],
    )
    async def test_a_script_that_fails_or_writes_bad_output_is_a_runner_error(self, script: str, message: str) -> None:
        with pytest.raises(MetricRunnerError, match=message):
            await LocalMetricInvoker().invoke("org-1", script, [{"reward": 1.0}])

    @pytest.mark.description("A script that runs too long is stopped.")
    async def test_a_script_that_runs_too_long_is_stopped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(metric_process, "SCRIPT_TIMEOUT_SECONDS", 1)

        with pytest.raises(MetricRunnerError, match="longer than 1 seconds"):
            await LocalMetricInvoker().invoke("org-1", "import time\ntime.sleep(60)\n", [])

    @pytest.mark.description("A missing uv is a runner error.")
    async def test_a_missing_uv_is_a_runner_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PATH", "")

        with pytest.raises(MetricRunnerError, match="uv could not be started"):
            await LocalMetricInvoker().invoke("org-1", HALVING_SCRIPT, [])


async def score_folder(folder: Path, results: Mapping[str, object]) -> Score:
    return await (await scoring.scorer_for(LocalDataset(path=str(folder)), ORG, None, mock.Mock())).score(results)


@pytest.mark.description("The scorer for a run on a dataset folder.")
class TestFolderScorer:
    @pytest.mark.description("A folder without a script is scored by the mean.")
    async def test_a_folder_without_a_script_is_scored_by_the_mean(self, tmp_path: Path) -> None:
        folder_scorer = await scoring.scorer_for(LocalDataset(path=str(tmp_path)), ORG, None, mock.Mock())

        assert isinstance(folder_scorer, MeanScorer)

    @pytest.mark.description("A folder with a script is scored by it and a changed script is a new digest.")
    async def test_a_folder_with_a_script_is_scored_by_it_and_a_changed_script_is_a_new_digest(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "metric.py").write_text(HALVING_SCRIPT)
        results = {"a": {"rewards": {"reward": 1.0}}, "b": {"rewards": {"reward": 0.0}}}

        first = await score_folder(tmp_path, results)
        (tmp_path / "metric.py").write_text(HALVING_SCRIPT.replace("/ 2", "/ 4"))
        changed = await score_folder(tmp_path, results)

        assert first.final_score == 25.0
        assert changed.final_score == 12.5
        assert changed.properties["scorer"]["digest"] != first.properties["scorer"]["digest"]

    @pytest.mark.description("An unreadable script leaves no score instead of the mean.")
    async def test_an_unreadable_script_leaves_no_score_instead_of_the_mean(self, tmp_path: Path) -> None:
        (tmp_path / "metric.py").write_bytes(b"\xff\xfe")

        score = await score_folder(tmp_path, {"a": {"rewards": {"reward": 1.0}}})

        assert score.final_score is None
        assert "could not be read" in score.properties["scorer"]["error"]
