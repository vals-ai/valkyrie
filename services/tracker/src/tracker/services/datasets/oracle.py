from tracker.types import AgentContractRequest

ORACLE_AGENT = "oracle"
SOLUTION_DIR = "solution"
SOLVE_SCRIPT = "solve.sh"
SOLUTION_REMOTE = "/solution"
ORACLE_CONTRACT = AgentContractRequest(
    name=ORACLE_AGENT,
    run_cmd=f'DEBIAN_FRONTEND=noninteractive bash {SOLUTION_REMOTE}/{SOLVE_SCRIPT}; echo "{SOLVE_SCRIPT} exited with $?"',
)
