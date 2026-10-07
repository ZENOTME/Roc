"""Export the exact measured Roc commits for the standalone benchmark."""
import io
import json
from pathlib import Path
import subprocess
import tarfile

root = Path(__file__).resolve().parent
heads = json.loads((root / "metadata.json").read_text())["heads"]
repo = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=root, text=True).strip()
for stage, commit in heads.items():
    destination = root / ".sources" / stage
    destination.mkdir(parents=True, exist_ok=True)
    data = subprocess.check_output(["git", "archive", commit, "Cargo.toml", "src"], cwd=repo)
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        archive.extractall(destination, filter="data")
    manifest = destination / "Cargo.toml"
    content = manifest.read_text().replace('name = "roc"', f'name = "roc_{stage}"')
    content = content[:content.index("[dev-dependencies]")]
    manifest.write_text(content)
