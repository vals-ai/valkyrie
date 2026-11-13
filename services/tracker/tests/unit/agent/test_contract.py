from pathlib import Path
from textwrap import dedent
from typing import Any, cast

import pytest
from pydantic import ValidationError
from tracker.agent.schemas import AgentConfig, AgentContract, Parameter
from tracker.types import AgentContractRequest, OutputArtifact

from tracker.agent.contract import _parse_yaml_contract  # type: ignore


def _make_contract(**overrides: Any) -> AgentContract:
    defaults: dict[str, Any] = {
        "name": "test_agent",
        "install_cmd": "bash setup.sh",
        "run_cmd": "agent --task {problem_statement_path}",
    }
    return AgentContract(**{**defaults, **overrides})


class TestValidateKwargs:
    @pytest.mark.description(
        (
            "Defaults are applied when no value is provided.\n"
            "\n"
            "Test cases:\n"
            "- Temperature has a default of 0.7 and the user provides no kwargs."
        )
    )
    def test_defaults_applied_when_no_values(self) -> None:
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {})

        assert result == {"temperature": 0.7}

    @pytest.mark.description(
        (
            "A user-provided value overrides the schema default.\n"
            "\n"
            "Test cases:\n"
            "- Temperature defaults to 0.7 but user passes 1.0 via kwargs."
        )
    )
    def test_override_replaces_default(self) -> None:
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"temperature": 1.0})

        assert result == {"temperature": 1.0}

    @pytest.mark.description(
        (
            "A missing required kwarg raises a ValidationError.\n"
            "\n"
            "Test cases:\n"
            "- Model is required but the user provides no kwargs."
        )
    )
    def test_required_kwarg_missing_raises(self) -> None:
        contract = _make_contract(
            kwargs={
                "model": Parameter(type="str", required=True),
            }
        )

        with pytest.raises(ValueError):
            contract.validate_kwargs(contract.kwargs, {})

    @pytest.mark.description(
        (
            "A required kwarg passes when the user provides it.\n"
            "\n"
            "Test cases:\n"
            '- Model is required and the user provides "gpt-4o".'
        )
    )
    def test_required_kwarg_provided(self) -> None:
        contract = _make_contract(
            kwargs={
                "model": Parameter(type="str", required=True),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"model": "gpt-4o"})

        assert result == {"model": "gpt-4o"}

    @pytest.mark.description(
        (
            "Passing a value with the wrong type raises a ValidationError.\n"
            "\n"
            "Test cases:\n"
            "- Temperature expects a float but the user provides a string."
        )
    )
    def test_wrong_type_raises(self) -> None:
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
            }
        )

        with pytest.raises(ValueError):
            contract.validate_kwargs(contract.kwargs, {"temperature": "not_a_float"})

    @pytest.mark.description(
        (
            "An empty kwargs schema returns an empty dict.\n"
            "\n"
            "Test cases:\n"
            "- Contract has no kwargs defined, values are ignored."
        )
    )
    def test_empty_schema_returns_empty(self) -> None:
        contract = _make_contract()

        result = contract.validate_kwargs({}, {"ignored": "value"})

        assert result == {}

    @pytest.mark.description(
        (
            "Defaults, required values, and overrides work together.\n"
            "\n"
            "Test cases:\n"
            "- Temperature and verbose have defaults, model is required and provided by the user."
        )
    )
    def test_multiple_kwargs_mixed(self) -> None:
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
                "model": Parameter(type="str", required=True),
                "verbose": Parameter(type="bool", required=False, default=True),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"model": "gpt-4o"})

        assert result == {"temperature": 0.7, "model": "gpt-4o", "verbose": True}

    @pytest.mark.description(
        (
            "An invalid choice raises a ValueError.\n"
            "\n"
            "Test cases:\n"
            '- Mode has choices ["fast", "accurate"] and "invalid" is rejected.'
        )
    )
    def test_choices_enforced(self) -> None:
        contract = _make_contract(
            kwargs={
                "mode": Parameter(type="str", required=False, default="fast", choices=["fast", "accurate"]),
            }
        )

        with pytest.raises(ValueError):
            contract.validate_kwargs(contract.kwargs, {"mode": "invalid"})

    @pytest.mark.description(
        'A valid choice is accepted.\n\nTest cases:\n- Mode has choices ["fast", "accurate"] and "fast" is accepted.'
    )
    def test_choices_valid(self) -> None:
        contract = _make_contract(
            kwargs={
                "mode": Parameter(type="str", required=False, default="fast", choices=["fast", "accurate"]),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"mode": "fast"})

        assert result == {"mode": "fast"}


class TestOutputArtifactValidation:
    @pytest.mark.description("An output artifact accepts a relative destination path.")
    def test_accepts_relative_destination_path(self) -> None:
        request = AgentContractRequest(
            name="test-agent",
            install_cmd="true",
            run_cmd="echo {problem_statement_path}",
            output_artifacts=["metrics/result.json"],
        )

        assert request.output_artifacts == ["metrics/result.json"]

    @pytest.mark.description("An output artifact accepts a single name as its destination.")
    def test_accepts_single_component_relative_destination(self) -> None:
        request = AgentContractRequest(
            name="test-agent",
            install_cmd="true",
            run_cmd="echo {problem_statement_path}",
            output_artifacts=["result.json"],
        )

        assert request.output_artifacts == ["result.json"]

    @pytest.mark.description("An output artifact destination with path traversal is rejected.")
    def test_rejects_path_traversal(self) -> None:
        with pytest.raises(ValidationError, match="output_artifacts"):
            AgentContractRequest(
                name="test-agent",
                install_cmd="true",
                run_cmd="echo {problem_statement_path}",
                output_artifacts=["artifacts/../secret.json"],
            )

    @pytest.mark.description("An absolute output artifact destination is rejected.")
    def test_rejects_absolute_path(self) -> None:
        with pytest.raises(ValidationError, match="relative"):
            AgentContractRequest(
                name="test-agent",
                install_cmd="true",
                run_cmd="echo {problem_statement_path}",
                output_artifacts=["/tmp/artifacts/result.json"],
            )

    @pytest.mark.description("A contract with too many output artifacts is rejected.")
    def test_rejects_too_many_output_artifacts(self) -> None:
        with pytest.raises(ValidationError, match="output_artifacts"):
            AgentContractRequest(
                name="test-agent",
                install_cmd="true",
                run_cmd="echo {problem_statement_path}",
                output_artifacts=["artifacts/result.json"] * 11,
            )

    @pytest.mark.description("An empty artifact source means the default source.")
    def test_empty_source_is_treated_as_default_source(self) -> None:
        artifact = OutputArtifact(path="artifacts/result.json", source="")

        assert artifact.source is None

    @pytest.mark.description("A relative artifact source path is rejected.")
    def test_rejects_relative_source_path(self) -> None:
        with pytest.raises(ValidationError, match="absolute sandbox paths"):
            OutputArtifact(path="artifacts/result.json", source="logs/result.json")

    @pytest.mark.description("An artifact source that globs the root is rejected.")
    def test_rejects_root_glob_source_path(self) -> None:
        with pytest.raises(ValidationError, match="non-root directory prefix"):
            OutputArtifact(path="artifacts/result.json", source="/*.json")

    @pytest.mark.description("An artifact with an explicit source and destination is accepted.")
    def test_accepts_explicit_source_and_destination(self) -> None:
        request = AgentContractRequest(
            name="test-agent",
            install_cmd="true",
            run_cmd="echo {problem_statement_path}",
            output_artifacts=[
                OutputArtifact(
                    path="artifacts/result.json",
                    source="/logs/{task_id}/result.json",
                )
            ],
        )

        artifact = request.output_artifacts[0]
        assert not isinstance(artifact, str)
        assert artifact.path == "artifacts/result.json"
        assert artifact.source == "/logs/{task_id}/result.json"


class TestRunCmdValidation:
    @pytest.mark.description(
        (
            "`run_cmd` without {problem_statement_path} raises a ValidationError.\n"
            "\n"
            "Test cases:\n"
            "- run_cmd has no placeholder for the problem statement path."
        )
    )
    def test_missing_problem_statement_path_raises(self) -> None:
        with pytest.raises(ValidationError, match="problem_statement_path"):
            _make_contract(run_cmd="agent --no-placeholder")

    @pytest.mark.description(
        (
            "A run_cmd containing {problem_statement_path} passes validation.\n"
            "\n"
            "Test cases:\n"
            "- run_cmd includes the required placeholder."
        )
    )
    def test_valid_run_cmd(self) -> None:
        contract = _make_contract(run_cmd="agent {problem_statement_path}")
        assert contract.run_cmd == "agent {problem_statement_path}"


@pytest.mark.description("End-to-end: YAML file -> AgentContractRequest with kwargs resolved.")
class TestParseYamlContract:
    @pytest.mark.description("Agent UID isolation is opt-in and survives YAML contract parsing.")
    @pytest.mark.parametrize("isolated", [False, True])
    def test_agent_uid_isolation(self, tmp_path: Path, isolated: bool) -> None:
        path = tmp_path / "agent.yaml"
        field = "\nisolate_agent_uid: true" if isolated else ""
        path.write_text('name: agent\ninstall_cmd: "true"\nrun_cmd: "cat {problem_statement_path}"' + field)

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.isolate_agent_uid is isolated

    def _write_yaml(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "contract.yaml"
        p.write_text(dedent(content))
        return p

    @pytest.mark.description("YAML defaults are validated and retained for launch-time binding.")
    def test_defaults_applied(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --temp {temperature}"
            kwargs:
              temperature:
                type: float
                required: false
                default: 0.7
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert isinstance(result, AgentContractRequest)
        assert result.kwargs == {"temperature": "0.7"}

    @pytest.mark.description("CLI kwargs override YAML defaults without changing the raw template.")
    def test_cli_kwargs_override_defaults(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --temp {temperature}"
            kwargs:
              temperature:
                type: float
                required: false
                default: 0.7
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(kwargs={"temperature": 1.0}))

        assert result.kwargs == {"temperature": "1.0"}

    @pytest.mark.description(
        (
            "A missing required kwarg raises during YAML contract parsing.\n"
            "\n"
            "Test cases:\n"
            "- Model is required in the YAML schema but not provided via AgentConfig."
        )
    )
    def test_required_kwarg_missing_raises(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model}"
            kwargs:
              model:
                type: str
                required: true
        """,
        )

        with pytest.raises(ValueError, match="is required but was not provided"):
            _parse_yaml_contract(path, AgentConfig())

    @pytest.mark.description(
        (
            "Secrets, final_output, and output_artifacts from YAML are carried to the request.\n"
            "\n"
            "Test cases:\n"
            "- YAML defines API_KEY secret, /artifacts final_output, and one direct output artifact."
        )
    )
    def test_secrets_final_output_and_output_artifacts_passed_through(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
            final_output: /artifacts
            output_artifacts:
              - artifacts/turns.jsonl
              - path: artifacts/result.json
                source: /logs/{task_id}/result.json
            egress_allowlist:
              - https://api.openai.com
              - https://github.com
            secrets:
              API_KEY: MySecretName
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.final_output == "/artifacts"
        assert result.output_artifacts[0] == "artifacts/turns.jsonl"
        artifact = cast(OutputArtifact, result.output_artifacts[1])
        assert artifact.path == "artifacts/result.json"
        assert artifact.source == "/logs/{task_id}/result.json"
        assert result.egress_allowlist == ["https://api.openai.com", "https://github.com"]
        assert result.secrets == {"API_KEY": "MySecretName"}

    @pytest.mark.description("The install egress policy passes through parsing unchanged.")
    def test_install_egress_policy_passed_through(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
            install_egress:
              - https://packages.example.com
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.install_egress == ["https://packages.example.com"]
        assert result.egress_allowlist == []

    @pytest.mark.description("Legacy egress settings get defaults and map to the allowlist.")
    def test_legacy_egress_defaults_and_allowlist_mapping(self, tmp_path: Path) -> None:
        unrestricted = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
        """,
        )
        unrestricted_result = _parse_yaml_contract(unrestricted, AgentConfig())

        assert unrestricted_result.install_egress is None
        assert unrestricted_result.egress_allowlist == []

        unrestricted.write_text(
            dedent(
                """\
                name: my_agent
                install_cmd: bash setup.sh
                run_cmd: "agent --task {problem_statement_path}"
                egress_allowlist:
                  - https://api.openai.com
                """
            )
        )
        legacy_result = _parse_yaml_contract(unrestricted, AgentConfig())

        assert legacy_result.install_egress is None
        assert legacy_result.egress_allowlist == ["https://api.openai.com"]

    @pytest.mark.description("Install egress can sit alongside a legacy run allowlist.")
    def test_install_egress_can_coexist_with_legacy_run_allowlist(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
            egress_allowlist:
              - https://api.openai.com
            install_egress:
              - https://packages.example.com
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.install_egress == ["https://packages.example.com"]
        assert result.egress_allowlist == ["https://api.openai.com"]

    @pytest.mark.description("The obsolete agent egress field is rejected.")
    @pytest.mark.parametrize("egress_value", ["null", '{install: "*"}', "{run: []}"])
    def test_rejects_obsolete_agent_egress(self, tmp_path: Path, egress_value: str) -> None:
        path = self._write_yaml(
            tmp_path,
            f"""\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {{problem_statement_path}}"
            egress: {egress_value}
        """,
        )

        with pytest.raises(ValueError, match="install_egress"):
            _parse_yaml_contract(path, AgentConfig())

    @pytest.mark.description(
        (
            "The model comes from AgentConfig, not the YAML contract.\n"
            "\n"
            "Test cases:\n"
            '- AgentConfig specifies "gpt-4o", YAML has no model field.'
        )
    )
    def test_model_from_agent_config(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(model="gpt-4o"))

        assert result.model == "gpt-4o"

    @pytest.mark.description(
        (
            "A YAML contract with no kwargs produces an unmodified run command.\n"
            "\n"
            "Test cases:\n"
            "- YAML defines no kwargs, run_cmd passes through with only built-in placeholders."
        )
    )
    def test_no_kwargs_in_schema(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.run_cmd == "agent --task {problem_statement_path}"

    @pytest.mark.description("CLI --model is validated as a required default and retained for launch.")
    def test_defaults_model_from_cli(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model}"
            defaults:
              model:
                type: str
                required: true
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(model="gpt-4o"))

        assert result.model == "gpt-4o"
        assert result.kwargs == {"model": "gpt-4o"}

    @pytest.mark.description(
        (
            "A required default raises when --model is not provided.\n"
            "\n"
            "Test cases:\n"
            "- Model is required in defaults but no --model flag is passed."
        )
    )
    def test_defaults_model_required_missing_raises(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model}"
            defaults:
              model:
                type: str
                required: true
        """,
        )

        with pytest.raises(ValueError, match="is required but was not provided"):
            _parse_yaml_contract(path, AgentConfig())

    @pytest.mark.description("Model defaults and kwargs are validated independently of command binding.")
    def test_defaults_merged_with_kwargs(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model} --temp {temperature}"
            defaults:
              model:
                type: str
                required: true
            kwargs:
              temperature:
                type: float
                required: false
                default: 0.7
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(model="gpt-4o"))

        assert result.model == "gpt-4o"
        assert result.kwargs == {"model": "gpt-4o", "temperature": "0.7"}

    @pytest.mark.description(
        (
            "The provided section is stripped and does not cause a parse error.\n"
            "\n"
            "Test cases:\n"
            "- YAML includes a provided section for documentation, it is silently ignored."
        )
    )
    def test_provided_section_ignored(self, tmp_path: Path) -> None:
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
            provided:
              task_id:
                type: str
                required: false
              problem_statement_path:
                type: str
                required: true
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.name == "my_agent"
