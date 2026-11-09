from collections.abc import Callable, Sequence
from urllib.parse import urlparse

import click
from valkyrie.sdk import BenchmarkServiceEntry, BenchmarkServiceHealth

from valkyrie.cli.client import call
from valkyrie.cli.display import format_table, paginate_cli_pages


@click.group(help="Save benchmark service URLs that your runs use instead of the hosted services.")
def service() -> None:
    pass


@service.command(
    "set",
    help=(
        "Use a service URL for your runs of a benchmark.\n"
        "\n"
        "Replaces the URL and credential saved before. The Tracker must be able to reach the URL.\n"
        "\n"
        "Example:\n"
        "    valkyrie benchmark service set swebench https://my-tunnel.ngrok.io --auth-secret swebench-key"
    ),
)
@click.argument("benchmark")
@click.argument("url")
@click.option(
    "--auth-secret",
    metavar="SECRET",
    help="Organization secret whose value the Tracker sends to the service. Create it with `valkyrie secret set`.",
)
@click.option("--auth-header", metavar="HEADER", help="Header that carries the secret. Defaults to Authorization.")
def service_set(benchmark: str, url: str, auth_secret: str | None, auth_header: str | None) -> None:
    settings = call(
        lambda client: client.settings.set_benchmark_service(
            benchmark, url, auth_secret=auth_secret, auth_header=auth_header
        )
    )
    saved = settings.benchmark_services[benchmark]
    message = f"Runs of {benchmark} now use {saved.url}"
    if saved.auth_secret_name:
        message += f", sending the secret {saved.auth_secret_name} as {saved.auth_header_name}"
    click.echo(f"{message}.")


@service.command(
    "remove",
    help=(
        "Go back to the hosted service for your runs of a benchmark.\n"
        "\n"
        "Example:\n"
        "    valkyrie benchmark service remove swebench"
    ),
)
@click.argument("benchmark")
def service_remove(benchmark: str) -> None:
    call(lambda client: client.settings.remove_benchmark_service(benchmark))
    click.echo(f"Runs of {benchmark} now use the hosted service.")


@service.command(
    "list",
    help=(
        "List hosted benchmark services and your saved URLs, with their latency.\n"
        "\n"
        "A URL you saved replaces the hosted service with the same name."
    ),
)
def service_list() -> None:
    saved = call(lambda client: client.settings.get()).benchmark_services

    catalog = call(lambda client: client.services.catalog())
    services_by_name = {service.name: service for service in catalog.services}
    services_by_name.update({name: BenchmarkServiceEntry(name=name, url=entry.url) for name, entry in saved.items()})
    if not services_by_name:
        click.echo(click.style("No benchmark services configured.", fg="yellow"))
        return

    paginate_services(
        list(services_by_name.values()),
        check_services=lambda entries: call(lambda client: client.services.check(entries)).services,
    )


def _service_source_domain(service: BenchmarkServiceHealth) -> str:
    host = urlparse(service.url).netloc or service.url
    return host.removeprefix(f"{service.name}.")


def _health_checked_page(
    services: Sequence[BenchmarkServiceEntry],
    cache: dict[str, BenchmarkServiceHealth],
    check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]],
) -> list[BenchmarkServiceHealth]:
    missing = [service for service in services if service.name not in cache]
    if missing:
        cache.update({service.name: service for service in check_services(missing)})
    for service in missing:
        cache.setdefault(
            service.name, BenchmarkServiceHealth(name=service.name, url=service.url, healthy=False, latency_ms=None)
        )
    return [cache[service.name] for service in services]


def paginate_services(
    services: Sequence[BenchmarkServiceEntry],
    check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]],
    limit: int = 10,
) -> None:
    health_cache: dict[str, BenchmarkServiceHealth] = {}

    def load_page(offset: int, page_limit: int) -> tuple[int, list[BenchmarkServiceHealth]]:
        page_services = _health_checked_page(services[offset : offset + page_limit], health_cache, check_services)
        return len(services), page_services

    def render_page(
        page_services: list[BenchmarkServiceHealth],
        current_page: int,
        total_pages: int,
        total_count: int,
    ) -> None:
        rows = [
            {
                "Benchmark": service.name,
                "Service URL": service.url,
                "Source": _service_source_domain(service),
                "Latency": f"{service.latency_ms} ms" if service.latency_ms is not None else "-",
            }
            for service in page_services
        ]
        format_table(
            rows,
            ["Benchmark", "Service URL", "Source", "Latency"],
            current_page,
            total_pages,
            total_count,
            "service",
        )

    paginate_cli_pages(
        load_page,
        render_page,
        limit=limit,
        render_empty=lambda: click.echo(click.style("No benchmark services found.", fg="yellow")),
    )
