import pytest
from pydantic import ValidationError

from tracker.services.datasets.config import NetworkMode, TaskConfig, TaskOS

MINIMAL = """
schema_version = "1.4"

[agent]
timeout_sec = 600

[verifier]
timeout_sec = 60

[environment]
cpus = 1
memory_mb = 2048
workdir = "/app"
"""

MULTIMODAL = """
schema_version = "1.3"

[task]
name = "swe-bench/swebench-multimodal__django-1"
description = "SWE-bench Multimodal issue from django."
keywords = ["swe-bench", "javascript"]

[metadata]
category = "debugging"
repo = "django"

[agent]
timeout_sec = 3000.0

[verifier]
timeout_sec = 1800.0

[environment]
network_mode = "public"
build_timeout_sec = 1800.0
cpus = 4
memory_mb = 8192
storage_mb = 20480
"""

STAGED = """
schema_version = "1.4"

[agent]
timeout_sec = 1800
network_mode = "allowlist"
allowed_hosts = ["pypi.org", "files.pythonhosted.org"]

[verifier]
timeout_sec = 600
network_mode = "allowlist"
allowed_hosts = ["api.openai.com"]

[environment]
cpus = 2
"""


def _rejection(toml: str) -> str:
    with pytest.raises(ValidationError) as raised:
        TaskConfig.parse(toml)

    return str(raised.value)


@pytest.mark.description("Parsing a whole task.toml.")
class TestTaskConfig:
    @pytest.mark.description("A minimal task reads its values and defaults.")
    def test_a_minimal_task_reads_its_values_and_defaults(self) -> None:
        config = TaskConfig.parse(MINIMAL)

        assert config.schema_version == "1.4"
        assert (config.agent.timeout_sec, config.verifier.timeout_sec) == (600, 60)
        assert (config.environment.cpus, config.environment.memory_mb, config.environment.workdir) == (1, 2048, "/app")
        assert config.environment.network_mode == NetworkMode.PUBLIC
        assert config.environment.build_timeout_sec == 600
        assert config.agent.network_mode is None
        assert config.environment.os == TaskOS.LINUX

    @pytest.mark.description("An empty file is a task with every default.")
    def test_an_empty_file_is_a_task_with_every_default(self) -> None:
        config = TaskConfig.parse("")

        assert config.schema_version == "1.3"
        assert config.verifier.timeout_sec == 600
        assert config.agent.timeout_sec is None
        assert config.task is None

    @pytest.mark.description("Package info and metadata are kept.")
    def test_package_info_and_metadata_are_kept(self) -> None:
        config = TaskConfig.parse(MULTIMODAL)

        assert config.task is not None
        assert config.task.name == "swe-bench/swebench-multimodal__django-1"
        assert config.task.keywords == ["swe-bench", "javascript"]
        assert config.metadata == {"category": "debugging", "repo": "django"}
        assert (config.environment.cpus, config.environment.storage_mb) == (4, 20480)
        assert config.environment.build_timeout_sec == 1800

    @pytest.mark.description("Cpus may be fractional.")
    def test_cpus_may_be_fractional(self) -> None:
        assert TaskConfig.parse("[environment]\ncpus = 0.5").environment.cpus == 0.5

    @pytest.mark.description("A size that is not positive is rejected.")
    @pytest.mark.parametrize(
        ("setting", "complaint"),
        [
            ("cpus = 0", "greater than 0"),
            ("cpus = nan", "finite number"),
            ("memory_mb = 0", "greater than 0"),
            ("storage_mb = -1", "greater than 0"),
        ],
    )
    def test_a_size_that_is_not_positive_is_rejected(self, setting: str, complaint: str) -> None:
        assert complaint in _rejection(f"[environment]\n{setting}")

    @pytest.mark.description("Each stage can set its own network rules.")
    def test_each_stage_can_set_its_own_network_rules(self) -> None:
        config = TaskConfig.parse(STAGED)

        assert config.agent.network_mode == NetworkMode.ALLOWLIST
        assert config.agent.allowed_hosts == ["pypi.org", "files.pythonhosted.org"]
        assert config.verifier.allowed_hosts == ["api.openai.com"]
        assert config.environment.network_mode == NetworkMode.PUBLIC

    @pytest.mark.description("Hosts are lowercased and stripped.")
    def test_hosts_are_lowercased_and_stripped(self) -> None:
        config = TaskConfig.parse(
            '[agent]\nnetwork_mode = "allowlist"\nallowed_hosts = ["PyPI.org.", "*.Example.com", "10.0.0.1", " 1.1.1.0/24 "]'
        )

        assert config.agent.allowed_hosts == ["pypi.org", "*.example.com", "10.0.0.1", "1.1.1.0/24"]

    @pytest.mark.description("Malformed allowed hosts are rejected.")
    @pytest.mark.parametrize(
        ("hosts", "complaint"),
        [
            ('["https://pypi.org"]', "not URLs, ports or paths"),
            ('["pypi.org:443"]', "not URLs, ports or paths"),
            ('["pypi.org/simple"]', "not URLs, ports or paths"),
            ('["example.com/24"]', "not URLs, ports or paths"),
            ('["1.1.1.0/33"]', "not URLs, ports or paths"),
            ('["1.1.1.5/24"]', "not URLs, ports or paths"),
            ('["1.1.1.0/255.255.255.0"]', "not URLs, ports or paths"),
            ('["10.0.0.1/0.0.0.0"]', "not URLs, ports or paths"),
            ('["1.1.1.0/24."]', "not URLs, ports or paths"),
            ('["2001:db8::/32"]', "not URLs, ports or paths"),
            ('["1.1.1.0:443/24"]', "not URLs, ports or paths"),
            ('["a*b.com"]', "leading '*.'"),
            ('["*."]', "leading '*.'"),
            ('[""]', "non-empty"),
            ('["bad_host"]', "letters, digits, hyphens and dots"),
            ('["evil.com\\n.attacker.com"]', "letters, digits, hyphens and dots"),
        ],
    )
    def test_malformed_allowed_hosts_are_rejected(self, hosts: str, complaint: str) -> None:
        message = _rejection(f'[agent]\nnetwork_mode = "allowlist"\nallowed_hosts = {hosts}')

        assert complaint in message

    @pytest.mark.description("Allowed hosts need an allowlist.")
    @pytest.mark.parametrize("section", ["agent", "verifier", "environment"])
    def test_allowed_hosts_need_an_allowlist(self, section: str) -> None:
        message = _rejection(f'[{section}]\nnetwork_mode = "public"\nallowed_hosts = ["pypi.org"]')

        assert "only valid when network_mode='allowlist'" in message

    @pytest.mark.description("Any schema version text is kept, and the older `version` key names it too.")
    @pytest.mark.parametrize(
        ("toml", "version"),
        [
            ('schema_version = "0.0"', "0.0"),
            ('schema_version = "2.0"', "2.0"),
            ('schema_version = "next"', "next"),
            ('version = "1.2"', "1.2"),
        ],
    )
    def test_the_schema_version_text_is_kept(self, toml: str, version: str) -> None:
        assert TaskConfig.parse(toml).schema_version == version

    @pytest.mark.description("Os is case insensitive.")
    def test_os_is_case_insensitive(self) -> None:
        assert TaskConfig.parse('[environment]\nos = "Windows"').environment.os == TaskOS.WINDOWS


@pytest.mark.description("Field names older task folders use.")
class TestOlderEnvironmentNames:
    @pytest.mark.description("Memory and storage sizes become megabytes.")
    def test_memory_and_storage_sizes_become_megabytes(self) -> None:
        environment = TaskConfig.parse('[environment]\nmemory = "2G"\nstorage = "512M"').environment

        assert (environment.memory_mb, environment.storage_mb) == (2048, 512)

    @pytest.mark.description("Allow internet picks a network mode.")
    @pytest.mark.parametrize(("allowed", "mode"), [("true", NetworkMode.PUBLIC), ("false", NetworkMode.NO_NETWORK)])
    def test_allow_internet_picks_a_network_mode(self, allowed: str, mode: NetworkMode) -> None:
        assert TaskConfig.parse(f"[environment]\nallow_internet = {allowed}").environment.network_mode == mode

    @pytest.mark.description("An explicit network mode wins over allow internet.")
    def test_an_explicit_network_mode_wins_over_allow_internet(self) -> None:
        config = TaskConfig.parse('[environment]\nallow_internet = false\nnetwork_mode = "public"')

        assert config.environment.network_mode == NetworkMode.PUBLIC


@pytest.mark.description("Whether grading runs in the agent's sandbox or its own.")
class TestVerifierEnvironment:
    @pytest.mark.description(
        "An environment or the separate mode makes the verifier separate, and it is shared otherwise."
    )
    def test_the_verifier_is_separate_only_when_asked(self) -> None:
        config = TaskConfig.parse('[verifier.environment]\ndocker_image = "grader:1"')

        assert config.verifier.is_separate
        assert config.verifier.environment is not None
        assert config.verifier.environment.docker_image == "grader:1"
        assert TaskConfig.parse('[verifier]\nenvironment_mode = "separate"').verifier.is_separate
        assert not TaskConfig.parse(MINIMAL).verifier.is_separate


@pytest.mark.description("The [task] table.")
class TestPackageInfo:
    @pytest.mark.description("Names must look like org slash name.")
    @pytest.mark.parametrize("name", ["no-org", "a/b/c", ".hidden/x", "org/..", "bad name/x", "org/name\\n"])
    def test_names_must_look_like_org_slash_name(self, name: str) -> None:
        assert "'org/name'" in _rejection(f'[task]\nname = "{name}"')


@pytest.mark.description("The healthcheck, MCP servers and users a task asks the sandbox for.")
class TestRuntimeSettings:
    @pytest.mark.description("A healthcheck takes docker style defaults.")
    def test_a_healthcheck_takes_docker_style_defaults(self) -> None:
        config = TaskConfig.parse("[environment.healthcheck]\ncommand = 'pg_isready'\nretries = 5")

        healthcheck = config.environment.healthcheck
        assert healthcheck is not None
        assert (healthcheck.command, healthcheck.retries) == ("pg_isready", 5)
        assert (healthcheck.interval_sec, healthcheck.timeout_sec, healthcheck.start_period_sec) == (5.0, 30.0, 0.0)

    @pytest.mark.description("Mcp servers accept http as streamable http.")
    def test_mcp_servers_accept_http_as_streamable_http(self) -> None:
        config = TaskConfig.parse(
            "[[environment.mcp_servers]]\nname = 'web'\ntransport = 'http'\nurl = 'https://x'\n"
            "[[environment.mcp_servers]]\nname = 'local'\ntransport = 'stdio'\ncommand = 'npx'\nargs = ['-y', 'srv']"
        )

        assert [(server.name, server.transport) for server in config.environment.mcp_servers] == [
            ("web", "streamable-http"),
            ("local", "stdio"),
        ]

    @pytest.mark.description("An mcp server without its target is rejected.")
    @pytest.mark.parametrize(
        ("server", "complaint"),
        [
            ("name = 'a'\ntransport = 'stdio'", "needs a command"),
            ("name = 'a'", "needs a url"),
            ("name = 'a'\ntransport = 'carrier-pigeon'", "transport"),
        ],
    )
    def test_an_mcp_server_without_its_target_is_rejected(self, server: str, complaint: str) -> None:
        assert complaint in _rejection(f"[[environment.mcp_servers]]\n{server}")

    @pytest.mark.description("A credential header may have outer spaces and adjacent references.")
    @pytest.mark.parametrize("value", ["Bearer ${TOKEN} ", " Bearer ${TOKEN}", "${A}${B}", "Token ${KEY_ID}:${SECRET}"])
    def test_a_credential_header_may_have_outer_spaces_and_adjacent_references(self, value: str) -> None:
        config = TaskConfig.parse(
            f"[[environment.mcp_servers]]\nname = 'web'\nurl = 'https://x'\nheaders = {{ Authorization = '{value}' }}"
        )

        assert config.environment.mcp_servers[0].headers == {"Authorization": value}

    @pytest.mark.description("An mcp server header with a literal credential or a malformed value is rejected.")
    @pytest.mark.parametrize(
        ("headers", "complaint"),
        [
            ("{ Authorization = 'Bearer abc' }", "must reference its credential"),
            ("{ Authorization = 'Bearer' }", "must reference its credential"),
            ("{ Cookie = 'session=abc' }", "must reference its credential"),
            ("{ X-Api-Key = '${KEY:-abc}' }", "must not carry a default"),
            ("{ X-Trace = '${SECRET_ID:-abc}' }", "must not carry a default"),
            ("{ X-Trace = '${1BAD}' }", "malformed"),
            ("{ X-Trace = '${A:-${B}}' }", "malformed"),
            ("{ Authorization = 'Bearer ${A:-${TOKEN}}' }", "malformed"),
            ("{ 'bad name' = 'x' }", "not a valid HTTP header name"),
            (r'{ X-Trace = "a\nb" }', "newlines"),
        ],
    )
    def test_an_mcp_server_header_with_a_literal_credential_or_a_malformed_value_is_rejected(
        self, headers: str, complaint: str
    ) -> None:
        assert complaint in _rejection(
            f"[[environment.mcp_servers]]\nname = 'web'\nurl = 'https://x'\nheaders = {headers}"
        )

    @pytest.mark.description("A later mcp server replaces an earlier one of the same name in the first ones place.")
    def test_a_later_mcp_server_replaces_an_earlier_one_of_the_same_name_in_the_first_ones_place(self) -> None:
        toml = (
            "[[environment.mcp_servers]]\nname = 'a'\nurl = 'https://first'\n"
            "[[environment.mcp_servers]]\nname = 'b'\nurl = 'https://b'\n"
            "[[environment.mcp_servers]]\nname = 'a'\nurl = 'https://second'\n"
        )

        servers = TaskConfig.parse(toml).environment.mcp_servers

        assert [(server.name, server.url) for server in servers] == [("a", "https://second"), ("b", "https://b")]

    @pytest.mark.description("Users may be names or uids and are kept as text.")
    def test_users_may_be_names_or_uids_and_are_kept_as_text(self) -> None:
        config = TaskConfig.parse("[agent]\nuser = 'agent'\n[verifier]\nuser = 1000")

        assert (config.agent.user, config.verifier.user) == ("agent", "1000")

    @pytest.mark.description("A user that could reach a shell or an option is rejected.")
    @pytest.mark.parametrize(
        "user",
        ["'$(id)'", "'a b'", "''", "'-sfiles'", "-1", "true", "'x\"; touch f; echo \"'"],
        ids=["substitution", "space", "empty", "option", "negative", "bool", "quote"],
    )
    def test_a_user_that_could_reach_a_shell_or_an_option_is_rejected(self, user: str) -> None:
        assert "user" in _rejection(f"[agent]\nuser = {user}")

    @pytest.mark.description("A healthcheck the runtime cannot honor is rejected.")
    @pytest.mark.parametrize(
        ("setting", "value"),
        [
            ("retries", "0"),
            ("interval_sec", "-1"),
            ("timeout_sec", "0"),
            ("start_period_sec", "-1"),
            ("start_interval_sec", "0"),
            ("timeout_sec", "nan"),
            ("start_period_sec", "inf"),
        ],
    )
    def test_a_healthcheck_the_runtime_cannot_honor_is_rejected(self, setting: str, value: str) -> None:
        assert setting in _rejection(f"[environment.healthcheck]\ncommand = 'ready'\n{setting} = {value}")


@pytest.mark.description("The files and commands a task asks to have collected.")
class TestArtifacts:
    @pytest.mark.description("An artifact is a path or a table and names its archive.")
    def test_an_artifact_is_a_path_or_a_table_and_names_its_archive(self) -> None:
        config = TaskConfig.parse(
            "artifacts = ['/app/out/', './results', { source = 'logs', destination = 'run-logs', exclude = ['*.tmp'] }]"
        )

        assert [artifact.name for artifact in config.artifacts] == ["app/out", "results", "run-logs"]
        assert config.artifacts[2].exclude == ["*.tmp"]

    @pytest.mark.description("A relative source may climb when it has a destination.")
    def test_a_relative_source_may_climb_when_it_has_a_destination(self) -> None:
        config = TaskConfig.parse("artifacts = [{ source = '../logs', destination = 'logs' }]")

        assert config.artifacts[0].name == "logs"

    @pytest.mark.description("An artifact that could escape or cannot be stored is rejected.")
    @pytest.mark.parametrize(
        ("artifact", "complaint"),
        [
            ("'/app/../etc'", "must not contain '..'"),
            ("{ source = '/app/../etc' }", "must not contain '..'"),
            ("'/'", "ordinary parts"),
            ("{ source = '/app', destination = '/abs' }", "ordinary parts"),
            ("{ source = '/app', destination = '../up' }", "ordinary parts"),
            ("{ source = '/app', destination = 'a//b' }", "ordinary parts"),
            ("{ source = '/app', destination = 'a\\\\b' }", "ordinary parts"),
            ("{ source = '/app', destination = '" + "x" * 300 + "' }", "ordinary parts"),
            ("{ source = 'rel', service = 'db' }", "needs an absolute source"),
            ("{ source = '/app', service = 'bad name' }", "service names"),
            ("{ source = '/app', excludes = ['*.key'] }", "Extra inputs are not permitted"),
        ],
    )
    def test_an_artifact_that_could_escape_or_cannot_be_stored_is_rejected(self, artifact: str, complaint: str) -> None:
        assert complaint in _rejection(f"artifacts = [{artifact}]")

    @pytest.mark.description("Two artifacts may not share a name and there is a limit.")
    def test_two_artifacts_may_not_share_a_name_and_there_is_a_limit(self) -> None:
        assert "same name" in _rejection("artifacts = ['/a/out', { source = '/b', destination = 'a/out' }]")
        assert "at most 20" in _rejection("artifacts = [" + ", ".join(f"'/f{index}'" for index in range(21)) + "]")

    @pytest.mark.description("Collect hooks default to the main service and a minute.")
    def test_collect_hooks_default_to_the_main_service_and_a_minute(self) -> None:
        config = TaskConfig.parse("[[verifier.collect]]\ncommand = 'sync'")

        hook = config.verifier.collect[0]
        assert (hook.service, hook.timeout_sec, hook.user) == ("main", 60.0, None)


@pytest.mark.description("The steps a task is split into, each with the settings it changes.")
class TestSteps:
    @pytest.mark.description("A task has no steps unless it lists them.")
    def test_a_task_has_no_steps_unless_it_lists_them(self) -> None:
        config = TaskConfig.parse("[environment]\ncpus = 1")

        assert config.steps == []
        assert config.reward_strategy.value == "mean"

    @pytest.mark.description("A step names only the settings it changes.")
    def test_a_step_names_only_the_settings_it_changes(self) -> None:
        config = TaskConfig.parse(
            "multi_step_reward_strategy = 'final'\n[[steps]]\nname = 'first'\nmin_reward = { accuracy = 0.9 }\n"
            "[steps.agent]\ntimeout_sec = 30\n[steps.healthcheck]\ncommand = 'ready'\n"
            "[[steps]]\nname = 'second'\nmin_reward = 0.5"
        )

        first, second = config.steps
        assert config.reward_strategy.value == "final"
        assert first.min_reward == {"accuracy": 0.9}
        assert first.agent.model_fields_set == {"timeout_sec"}
        assert first.healthcheck is not None and first.healthcheck.command == "ready"
        assert second.min_reward == 0.5
        assert second.verifier.model_fields_set == set()

    @pytest.mark.description("Clashing or unsafe steps are rejected.")
    @pytest.mark.parametrize(
        ("steps", "complaint"),
        [
            ("[[steps]]\nname = 'a'\n[[steps]]\nname = 'A'", "share a name"),
            ("[[steps]]\nname = 'a/b'", "folder name"),
            ("[[steps]]\nname = ''", "folder name"),
            ("[[steps]]\nname = 'a'\nmin_reward = inf", "finite"),
            ("[[steps]]\nname = 'a'\nartifacts = ['/x', '/x']", "same name"),
        ],
    )
    def test_clashing_or_unsafe_steps_are_rejected(self, steps: str, complaint: str) -> None:
        assert complaint in _rejection(steps)


@pytest.mark.description("Settings that cannot be honored are rejected with the reason.")
@pytest.mark.parametrize(
    ("toml", "complaint"),
    [
        ('[environment]\nnetwork_mode = "tunnel"', "network_mode"),
        ('[environment]\nmemory = "2G"\nmemory_mb = 1024', "Conflicting"),
        ('[environment]\nmemory = "2"', "Invalid size"),
        ('[verifier]\nenvironment_mode = "shared"\n[verifier.environment]\ndocker_image = "grader:1"', "incompatible"),
        ('[task]\ndescription = "no name"', "name"),
        (
            "[[environment.mcp_servers]]\nname = 'a'\ntransport = 'stdio'\ncommand = 'x'\nheaders = { X = 'y' }",
            "headers are only for an MCP server over http",
        ),
        ("[[verifier.collect]]\ncommand = 'sync'\ntimeout = 5", "Extra inputs are not permitted"),
    ],
    ids=[
        "network-mode",
        "conflicting-size",
        "size-without-unit",
        "shared-with-environment",
        "task-without-name",
        "stdio-headers",
        "misspelled-hook",
    ],
)
def test_settings_that_cannot_be_honored_are_rejected(toml: str, complaint: str) -> None:
    assert complaint in _rejection(toml)
