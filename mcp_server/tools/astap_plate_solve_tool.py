from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import shutil
from pathlib import Path

from pydantic import BaseModel, Field


class Tools:
    """OpenWebUI-compatible ASTAP plate-solving helper for saved FITS files."""

    _DOWNLOADS_DIR = Path("data/downloads")
    _DEFAULT_BINARY_CANDIDATES = (
        "/opt/astap/astap_cli",
        "/opt/astap/astap",
        "/usr/local/bin/astap_cli",
        "/usr/local/bin/astap",
        "astap_cli",
        "astap",
    )

    class Valves(BaseModel):
        astap_binary_path: str = Field(
            default="/opt/astap/astap",
            description="Path to the ASTAP executable inside the container.",
        )
        astap_timeout_seconds: int = Field(
            default=180,
            ge=5,
            le=3600,
            description="Maximum time to wait for ASTAP solve command completion.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None) -> None:
        self.valves = valves or self.Valves()

    def _resolve_astap_binary(self) -> str | None:
        candidates: list[str] = []
        explicit = str(getattr(self.valves, "astap_binary_path", "") or "").strip()
        if explicit:
            candidates.append(explicit)

        env_override = str(os.environ.get("ASTAP_BINARY_PATH", "") or "").strip()
        if env_override:
            candidates.append(env_override)

        candidates.extend(self._DEFAULT_BINARY_CANDIDATES)

        seen: set[str] = set()
        for candidate in candidates:
            candidate = candidate.strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)

            candidate_path = Path(candidate)
            if candidate_path.is_absolute() or candidate_path.parent != Path("."):
                if candidate_path.exists() and os.access(candidate_path, os.X_OK):
                    return str(candidate_path)
                continue

            resolved = shutil.which(candidate)
            if resolved:
                return resolved

        return None

    async def astap_status(self) -> str:
        """Check whether ASTAP is available in the running container."""
        binary_path = self._resolve_astap_binary()
        candidates = list(dict.fromkeys([str(getattr(self.valves, "astap_binary_path", "") or "").strip(), *self._DEFAULT_BINARY_CANDIDATES]))

        if binary_path is None:
            return json.dumps(
                {
                    "available": False,
                    "binary": str(getattr(self.valves, "astap_binary_path", "") or "").strip() or None,
                    "binary_candidates": candidates,
                    "error": "ASTAP executable not found. Install ASTAP in the container or set astap_binary_path/ASTAP_BINARY_PATH.",
                }
            )

        cmd = [binary_path, "-h"]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except FileNotFoundError:
            return json.dumps(
                {
                    "available": False,
                    "binary": self.valves.astap_binary_path,
                    "error": "ASTAP executable not found in container PATH.",
                }
            )
        except Exception as exc:
            return json.dumps(
                {
                    "available": False,
                    "binary": self.valves.astap_binary_path,
                    "error": str(exc),
                }
            )

        text = (proc.stdout or "") + "\n" + (proc.stderr or "")
        return json.dumps(
            {
                "available": proc.returncode in (0, 1),
                "binary": binary_path,
                "binary_candidates": candidates,
                "return_code": proc.returncode,
                "output_preview": text.strip()[:800],
            }
        )

    async def astap_plate_solve(
        self,
        fits_filename: str,
    ) -> str:
        """Run ASTAP on an existing FITS file in data/downloads.

        :param fits_filename: Name of FITS file already present in data/downloads.
        :returns: JSON payload with ASTAP exit code, output, and solve heuristics.
        """
        filename = str(fits_filename or "").strip()
        if not filename:
            raise ValueError("fits_filename is required")

        requested = Path(filename)
        if requested.name != filename:
            raise ValueError("fits_filename must be a file name, not a path")

        suffix = requested.suffix.lower()
        if suffix not in {".fits", ".fit", ".fts"}:
            raise ValueError("fits_filename must end with .fits, .fit, or .fts")

        fits_path = self._DOWNLOADS_DIR / requested.name
        if not fits_path.exists() or not fits_path.is_file():
            raise ValueError(f"FITS file not found: {requested.name}")

        binary_path = self._resolve_astap_binary()
        if binary_path is None:
            raise RuntimeError(
                "ASTAP executable not found. Install ASTAP in the container or set astap_binary_path/ASTAP_BINARY_PATH."
            )

        cmd = [binary_path, "-f", str(fits_path.resolve())]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=int(self.valves.astap_timeout_seconds),
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "ASTAP executable not found. Install ASTAP in the container or set astap_binary_path valve."
            ) from exc

        combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        solved = self._looks_solved(combined, proc.returncode)
        solved_ra_hours, solved_dec_degrees = self._extract_solved_coordinates(combined)

        payload = {
            "fits_file": requested.name,
            "fits_file_url": f"/api/files/{requested.name}",
            "astap_command": " ".join(shlex.quote(part) for part in cmd),
            "return_code": proc.returncode,
            "solved": solved,
            "solved_ra_hours": solved_ra_hours,
            "solved_dec_degrees": solved_dec_degrees,
            "binary": binary_path,
            "stdout": (proc.stdout or "")[:4000],
            "stderr": (proc.stderr or "")[:4000],
        }
        return json.dumps(payload)

    @staticmethod
    def _looks_solved(output_text: str, return_code: int) -> bool:
        text = str(output_text or "").lower()
        if "no solution" in text or "failed" in text:
            return False
        if "solution" in text or "solved" in text or "wcs" in text:
            return True
        return return_code == 0

    @staticmethod
    def _extract_solved_coordinates(output_text: str) -> tuple[float | None, float | None]:
        text = str(output_text or "")

        ra_match = re.search(r"\bRA\s*[:=]\s*([0-9]{1,2}(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
        dec_match = re.search(r"\bDEC\s*[:=]\s*([+-]?[0-9]{1,2}(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)

        ra_val = float(ra_match.group(1)) if ra_match else None
        dec_val = float(dec_match.group(1)) if dec_match else None
        return ra_val, dec_val