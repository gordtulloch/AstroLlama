from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path


DEFAULT_MODEL_REPO = "bartowski/Qwen2.5-3B-Instruct-GGUF"
DEFAULT_MODEL_FILENAME = "Qwen2.5-3B-Instruct-Q8_0.gguf"
DEFAULT_OUTPUT_NAME = "qwen2.5-3b-instruct-q8_0.gguf"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download a model file from Hugging Face into the local ai/ directory. "
            "With no arguments, downloads the repo's default Qwen GGUF model."
        ),
    )
    parser.add_argument(
        "repo_id",
        nargs="?",
        default=os.getenv("HF_MODEL_REPO", DEFAULT_MODEL_REPO),
        help="Hugging Face repository id, for example bartowski/Qwen2.5-3B-Instruct-GGUF.",
    )
    parser.add_argument(
        "filename",
        nargs="?",
        default=os.getenv("HF_MODEL_FILE", DEFAULT_MODEL_FILENAME),
        help="File to download from the repository, for example Qwen2.5-3B-Instruct-Q8_0.gguf.",
    )
    parser.add_argument(
        "--revision",
        default=os.getenv("HF_MODEL_REVISION", "main"),
        help="Repository revision, branch, or tag. Defaults to main.",
    )
    parser.add_argument(
        "--target-dir",
        default="ai",
        help="Directory where the downloaded model should be copied. Defaults to ai.",
    )
    parser.add_argument(
        "--output-name",
        default=os.getenv("LLAMA_MODEL_FILE", DEFAULT_OUTPUT_NAME),
        help="Optional local file name to use instead of the source filename.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("HF_TOKEN"),
        help="Optional Hugging Face token. Defaults to HF_TOKEN from the environment.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the local target file if it already exists.",
    )
    return parser


def download_model(
    repo_id: str,
    filename: str,
    revision: str,
    target_dir: str,
    output_name: str | None,
    token: str | None,
    force: bool,
) -> Path:
    try:
        from huggingface_hub import hf_hub_download
    except ImportError as exc:
        raise SystemExit(
            "huggingface_hub is required. Install dependencies with 'pip install -r requirements.txt'."
        ) from exc

    target_path = Path(target_dir).resolve()
    target_path.mkdir(parents=True, exist_ok=True)

    destination = target_path / (output_name or Path(filename).name)
    if destination.exists() and not force:
        raise SystemExit(
            f"Refusing to overwrite existing file: {destination}. Use --force to replace it."
        )

    cached_path = Path(
        hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            revision=revision,
            token=token,
        )
    )

    shutil.copy2(cached_path, destination)
    return destination


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    destination = download_model(
        repo_id=args.repo_id,
        filename=args.filename,
        revision=args.revision,
        target_dir=args.target_dir,
        output_name=args.output_name,
        token=args.token,
        force=args.force,
    )

    print(f"Downloaded model to {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())