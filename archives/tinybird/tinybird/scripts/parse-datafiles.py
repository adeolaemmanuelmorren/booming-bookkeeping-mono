from pathlib import Path

from tinybird.datafile.parse_datasource import parse_datasource
from tinybird.datafile.parse_pipe import parse_pipe


def parse_project() -> None:
    project_root = Path.cwd()
    datasource_files = sorted(project_root.rglob("*.datasource"))
    pipe_files = sorted(project_root.rglob("*.pipe"))

    for path in datasource_files:
        parse_datasource(str(path))

    for path in pipe_files:
        parse_pipe(str(path))

    total = len(datasource_files) + len(pipe_files)
    print(
        f"Parsed {total} native datafiles: "
        f"{len(datasource_files)} Data Sources and {len(pipe_files)} Pipes."
    )


if __name__ == "__main__":
    parse_project()
