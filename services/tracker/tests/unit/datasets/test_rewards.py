import math

import pytest

from tracker.services.datasets.rewards import (
    extract_rewards,
    final_score,
    mean_rewards,
    normalize_rewards,
    primary_score,
)


def result(**rewards: float) -> dict[str, object]:
    return {"task_id": "t", "rewards": rewards}


@pytest.mark.description("Finding the rewards in whatever shape a result was stored.")
class TestExtractRewards:
    @pytest.mark.description("The verifier result holds them.")
    def test_the_verifier_result_holds_them(self) -> None:
        assert extract_rewards(result(reward=1.0, tests_passed=4)) == {"reward": 1.0, "tests_passed": 4.0}

    @pytest.mark.description("Anything without rewards has none.")
    @pytest.mark.parametrize("value", [None, "text", [], {}, {"verifier_result": {}}, {"rewards": []}])
    def test_anything_without_rewards_has_none(self, value: object) -> None:
        assert extract_rewards(value) is None

    @pytest.mark.description("A reward that is not a number is rejected.")
    @pytest.mark.parametrize("bad", [True, None, [1], {"a": 1}])
    def test_a_reward_that_is_not_a_number_is_rejected(self, bad: object) -> None:
        with pytest.raises(TypeError, match="numeric reward"):
            normalize_rewards({"reward": bad})

    @pytest.mark.description("A reward that is not finite is rejected.")
    @pytest.mark.parametrize("bad", ["nan", "inf", float("-inf")])
    def test_a_reward_that_is_not_finite_is_rejected(self, bad: object) -> None:
        with pytest.raises(ValueError, match="not a finite number"):
            normalize_rewards({"reward": bad})


@pytest.mark.description("Averaging over tasks, where an errored task counts as zero.")
class TestMeanRewards:
    @pytest.mark.description("A single reward key averages whatever it is called.")
    def test_a_single_reward_key_averages_whatever_it_is_called(self) -> None:
        means = mean_rewards([{"score": 1.0}, {"score": 0.0}, None])

        assert list(means) == ["mean"]
        assert math.isclose(means["mean"], 1 / 3)

    @pytest.mark.description("Several keys average per key with missing ones as zero.")
    def test_several_keys_average_per_key_with_missing_ones_as_zero(self) -> None:
        means = mean_rewards([{"reward": 1.0, "speed": 2.0}, {"reward": 0.0}, None])

        assert list(means) == ["reward", "speed"]
        assert math.isclose(means["reward"], 1 / 3)
        assert math.isclose(means["speed"], 2 / 3)

    @pytest.mark.description("No tasks average to zero.")
    def test_no_tasks_average_to_zero(self) -> None:
        assert mean_rewards([]) == {"mean": 0.0}


@pytest.mark.description("Which mean is the run's score.")
class TestPrimaryScore:
    @pytest.mark.description("The reward key wins.")
    def test_the_reward_key_wins(self) -> None:
        assert primary_score({"reward": 0.25, "speed": 9.0}) == (0.25, "reward")

    @pytest.mark.description("A lone mean is the score.")
    def test_a_lone_mean_is_the_score(self) -> None:
        assert primary_score({"mean": 0.5}) == (0.5, "mean")

    @pytest.mark.description("Several keys without a reward have no headline.")
    def test_several_keys_without_a_reward_have_no_headline(self) -> None:
        assert primary_score({"a": 1.0, "b": 2.0}) is None


@pytest.mark.description("The run score and its breakdown.")
class TestFinalScore:
    @pytest.mark.description("The score is the mean reward times one hundred.")
    def test_the_score_is_the_mean_reward_times_one_hundred(self) -> None:
        score, metadata = final_score({"a": result(reward=1.0), "b": result(reward=0.0), "c": result(reward=1.0)})

        assert math.isclose(score, 200 / 3)
        assert metadata["total"] == 3
        assert metadata["n_trials"] == 3
        assert metadata["n_errors"] == 0
        assert metadata["score_key"] == "mean"
        assert metadata["reward_stats"] == {"reward": {1.0: ["a", "c"], 0.0: ["b"]}}

    @pytest.mark.description("Several keys without a reward finish with every mean and no headline score.")
    def test_several_keys_without_a_reward_finish_with_every_mean_and_no_headline_score(self) -> None:
        score, metadata = final_score({"a": result(accuracy=1.0, speed=0.5), "b": result(accuracy=0.0, speed=1.5)})

        assert score is None
        assert metadata["metrics"] == [{"accuracy": 0.5, "speed": 1.0}]
        assert metadata["score_key"] is None and metadata["mean_reward"] is None
        assert (metadata["total"], metadata["n_trials"], metadata["n_errors"]) == (2, 2, 0)

    @pytest.mark.description("Errored tasks count as zero and are counted.")
    def test_errored_tasks_count_as_zero_and_are_counted(self) -> None:
        score, metadata = final_score({"a": result(reward=1.0), "b": None, "c": {"error": "boom"}})

        assert math.isclose(score, 100 / 3)
        assert (metadata["n_trials"], metadata["n_errors"]) == (1, 2)

    @pytest.mark.description("Every task errored scores zero.")
    def test_every_task_errored_scores_zero(self) -> None:
        score, metadata = final_score({"a": None})

        assert score == 0
        assert metadata["n_errors"] == 1
