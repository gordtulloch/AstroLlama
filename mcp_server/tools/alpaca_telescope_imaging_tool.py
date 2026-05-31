from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import BaseModel, Field

class Tools:
    """OpenWebUI-compatible Alpaca telescope slew and imaging tool."""

    _OUTPUT_DIR = Path("data/downloads")
    _CATALOG_COMMON_NAMES = {
        "M45": "Pleiades",
    }
    _DEFAULT_ASTAP_BINARY_CANDIDATES = (
        "/opt/astap/astap",
        "/opt/astap/astap_cli",
        "/usr/local/bin/astap",
        "/usr/local/bin/astap_cli",
        "astap",
        "astap_cli",
    )
    _JOB_LOCK = threading.Lock()
    _JOBS: dict[str, dict[str, object]] = {}

    class Valves(BaseModel):
        """Global configuration for Alpaca capture behavior."""

        default_alpaca_address: str = Field(
            default="localhost:32323",
            description="Default Alpaca host:port endpoint.",
        )
        default_protocol: Literal["http", "https"] = Field(
            default="http",
            description="Transport protocol for Alpaca requests.",
        )
        default_exposure_seconds: float = Field(
            default=10.0,
            gt=0,
            le=7200,
            description="Default image exposure length in seconds when not provided per call.",
        )
        default_telescope_device_number: int = Field(
            default=0,
            ge=0,
            description="Default Alpaca telescope device number.",
        )
        default_camera_device_number: int = Field(
            default=0,
            ge=0,
            description="Default Alpaca camera device number.",
        )
        slew_timeout_seconds: int = Field(
            default=300,
            ge=5,
            le=3600,
            description="Maximum time to wait for slew completion.",
        )
        slew_poll_seconds: float = Field(
            default=1.0,
            ge=0.1,
            le=30.0,
            description="Polling interval while waiting for slew completion.",
        )
        exposure_timeout_seconds: int = Field(
            default=300,
            ge=1,
            le=7200,
            description="Maximum time to wait for image readiness.",
        )
        exposure_poll_seconds: float = Field(
            default=0.5,
            ge=0.1,
            le=30.0,
            description="Polling interval while waiting for image readiness.",
        )
        plate_solve_exposure_seconds: float = Field(
            default=2.0,
            gt=0,
            le=120.0,
            description="Exposure length in seconds for slew+plate-solve verification frames.",
        )
        astap_binary_path: str = Field(
            default="/opt/astap/astap",
            description="Path to ASTAP executable for plate solving.",
        )
        astap_timeout_seconds: int = Field(
            default=180,
            ge=5,
            le=3600,
            description="Maximum time to wait for ASTAP solve command completion.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None) -> None:
        self.valves = valves or self.Valves()

    async def alpaca_slew_and_capture(
        self,
        object_name: str | None = None,
        ra_hours: float | None = None,
        dec_degrees: float | None = None,
        protocol: Literal["http", "https"] | None = None,
        exposure_seconds: float | None = None,
        exposure_count: int = 1,
        light_frame: bool = True,
        file_stem: str | None = None,
        bin_x: int = 1,
        bin_y: int = 1,
        enable_tracking: bool = True,
    ) -> str:
        """Start an Alpaca capture job and return immediately.

        Provide either object_name OR both ra_hours and dec_degrees.
        This tool always uses the configured default Alpaca address and device numbers.
        Call alpaca_server_status first if you need to inspect the configured server.
        For completion, call alpaca_capture_job_status with the returned job_id.

        :param object_name: Optional object identifier to resolve (for example "M42" or "Betelgeuse").
        :param ra_hours: Right ascension in hours (0-24) when targeting by coordinates.
        :param dec_degrees: Declination in degrees (-90 to +90) when targeting by coordinates.
        :param protocol: Alpaca protocol, usually "http".
        :param exposure_seconds: Exposure length in seconds; defaults to valve value when omitted.
        :param exposure_count: Number of sequential exposures to capture.
        :param light_frame: True for light frame, False for dark frame.
        :param file_stem: Optional output FITS filename stem (without extension).
        :param bin_x: Camera X binning value.
        :param bin_y: Camera Y binning value.
        :param enable_tracking: If True, attempt to enable telescope tracking before slewing.
        :returns: Job submission payload including job_id.
        """
        return await self.alpaca_slew_and_capture_start(
            object_name=object_name,
            ra_hours=ra_hours,
            dec_degrees=dec_degrees,
            protocol=protocol,
            exposure_seconds=exposure_seconds,
            exposure_count=exposure_count,
            light_frame=light_frame,
            file_stem=file_stem,
            bin_x=bin_x,
            bin_y=bin_y,
            enable_tracking=enable_tracking,
        )

    async def alpaca_slew_and_capture_start(
        self,
        object_name: str | None = None,
        ra_hours: float | None = None,
        dec_degrees: float | None = None,
        protocol: Literal["http", "https"] | None = None,
        exposure_seconds: float | None = None,
        exposure_count: int = 1,
        light_frame: bool = True,
        file_stem: str | None = None,
        bin_x: int = 1,
        bin_y: int = 1,
        enable_tracking: bool = True,
    ) -> str:
        """Start a background Alpaca capture job and return immediately.

        Use alpaca_capture_job_status() with the returned job_id to check completion.

        :param object_name: Optional object identifier to resolve (for example "M42" or "Betelgeuse").
        :param ra_hours: Right ascension in hours (0-24) when targeting by coordinates.
        :param dec_degrees: Declination in degrees (-90 to +90) when targeting by coordinates.
        :param protocol: Alpaca protocol, usually "http".
        :param exposure_seconds: Exposure length in seconds; defaults to valve value when omitted.
        :param exposure_count: Number of sequential exposures to capture.
        :param light_frame: True for light frame, False for dark frame.
        :param file_stem: Optional output FITS filename stem (without extension).
        :param bin_x: Camera X binning value.
        :param bin_y: Camera Y binning value.
        :param enable_tracking: If True, attempt to enable telescope tracking before slewing.
        :returns: Job submission payload including job_id.
        """
        job_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()
        args = {
            "object_name": object_name,
            "ra_hours": ra_hours,
            "dec_degrees": dec_degrees,
            "protocol": protocol,
            "exposure_seconds": exposure_seconds,
            "exposure_count": exposure_count,
            "light_frame": light_frame,
            "file_stem": file_stem,
            "bin_x": bin_x,
            "bin_y": bin_y,
            "enable_tracking": enable_tracking,
        }

        with self._JOB_LOCK:
            self._JOBS[job_id] = {
                "job_id": job_id,
                "status": "running",
                "submitted_at": now,
                "updated_at": now,
                "result": None,
                "error": None,
                "traceback": None,
                "args": args,
            }

        thread = threading.Thread(
            target=self._run_background_capture_job,
            kwargs={"job_id": job_id, "args": args},
            daemon=True,
        )
        thread.start()

        return json.dumps(
            {
                "job_id": job_id,
                "status": "running",
                "message": "Capture started. Call alpaca_capture_job_status with this job_id for completion status.",
            }
        )

    async def alpaca_capture_job_status(self, job_id: str) -> str:
        """Get status for a previously started background capture job.

        :param job_id: Job id returned by alpaca_slew_and_capture_start.
        :returns: Job status payload including result or error when complete.
        """
        key = str(job_id or "").strip()
        if not key:
            raise ValueError("job_id is required")

        with self._JOB_LOCK:
            row = self._JOBS.get(key)

        if row is None:
            raise ValueError(f"Unknown job_id: {key}")
        return json.dumps(row)

    async def alpaca_camera_diagnostics(
        self,
        protocol: Literal["http", "https"] | None = None,
    ) -> str:
        """Read camera-reported sensor/color settings for FITS interpretation.

        This method does not start an exposure. Use it to discover sensor type,
        Bayer offsets, and related camera capabilities directly from the driver.

        :param protocol: Alpaca protocol, usually "http".
        :returns: JSON payload of camera diagnostics and suggested FITS metadata hints.
        """
        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        effective_camera_device_number = int(self.valves.default_camera_device_number)
        effective_protocol = protocol or self.valves.default_protocol

        try:
            from alpaca.camera import Camera
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        camera = Camera(endpoint, effective_camera_device_number, protocol=effective_protocol)

        try:
            camera.Connected = True
            payload = {
                "status": "ok",
                "endpoint": endpoint,
                "protocol": effective_protocol,
                "camera_device_number": effective_camera_device_number,
                "diagnostics": self._collect_camera_profile(camera),
            }
            return json.dumps(payload)
        finally:
            try:
                camera.Connected = False
            except Exception:
                pass

    async def alpaca_park_telescope(
        self,
        protocol: Literal["http", "https"] | None = None,
    ) -> str:
        """Park the configured Alpaca telescope.

        This uses the configured default Alpaca address and telescope device number.

        :param protocol: Alpaca protocol, usually "http".
        :returns: Confirmation that the telescope is parked.
        """
        return await asyncio.to_thread(self._park_telescope_sync, protocol)

    async def alpaca_unpark_telescope(
        self,
        protocol: Literal["http", "https"] | None = None,
    ) -> str:
        """Unpark the configured Alpaca telescope.

        This uses the configured default Alpaca address and telescope device number.

        :param protocol: Alpaca protocol, usually "http".
        :returns: Confirmation that the telescope is unparked.
        """
        return await asyncio.to_thread(self._unpark_telescope_sync, protocol)

    async def alpaca_slew_and_plate_solve(
        self,
        object_name: str | None = None,
        ra_hours: float | None = None,
        dec_degrees: float | None = None,
        protocol: Literal["http", "https"] | None = None,
        plate_solve_exposure_seconds: float | None = None,
        file_stem: str | None = None,
        bin_x: int = 1,
        bin_y: int = 1,
        enable_tracking: bool = True,
    ) -> str:
        """Slew the telescope to a target and plate-solve a short verification frame.

        This is a pre-imaging alignment verification step. It does not run the
        multi-exposure imaging workflow.

        :param object_name: Optional object identifier (for example "M45").
        :param ra_hours: Right ascension in hours when targeting by coordinates.
        :param dec_degrees: Declination in degrees when targeting by coordinates.
        :param protocol: Alpaca protocol, usually "http".
        :param plate_solve_exposure_seconds: Exposure length for verification frame.
        :param file_stem: Optional output FITS filename stem.
        :param bin_x: Camera X binning value.
        :param bin_y: Camera Y binning value.
        :param enable_tracking: If True, attempt to enable tracking before slewing.
        :returns: Plate-solve summary with solve status and saved file path.
        """
        return await asyncio.to_thread(
            self._run_slew_and_plate_solve,
            object_name,
            ra_hours,
            dec_degrees,
            protocol,
            plate_solve_exposure_seconds,
            file_stem,
            bin_x,
            bin_y,
            enable_tracking,
        )

    async def alpaca_plate_solve_current_position(
        self,
        protocol: Literal["http", "https"] | None = None,
        plate_solve_exposure_seconds: float | None = None,
        file_stem: str | None = None,
        bin_x: int = 1,
        bin_y: int = 1,
    ) -> str:
        """Plate-solve at the telescope's current pointing without slewing.

        This captures a short verification frame at the current telescope
        location and runs ASTAP plate solving. Imaging remains a separate step.

        :param protocol: Alpaca protocol, usually "http".
        :param plate_solve_exposure_seconds: Exposure length for verification frame.
        :param file_stem: Optional output FITS filename stem.
        :param bin_x: Camera X binning value.
        :param bin_y: Camera Y binning value.
        :returns: Plate-solve summary including telescope current coordinates.
        """
        return await asyncio.to_thread(
            self._run_plate_solve_current_position,
            protocol,
            plate_solve_exposure_seconds,
            file_stem,
            bin_x,
            bin_y,
        )

    def _park_telescope_sync(
        self,
        protocol: Literal["http", "https"] | None,
    ) -> str:
        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        effective_telescope_device_number = int(self.valves.default_telescope_device_number)
        effective_protocol = protocol or self.valves.default_protocol

        try:
            from alpaca.telescope import Telescope
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        telescope = Telescope(endpoint, effective_telescope_device_number, protocol=effective_protocol)

        try:
            telescope.Connected = True

            can_park = self._safe_getattr(telescope, "CanPark", default=True)
            if can_park is False:
                raise RuntimeError("Configured telescope reports CanPark=False")

            if bool(self._safe_getattr(telescope, "AtPark", default=False)):
                return "Alpaca telescope is already parked."

            telescope.Park()
            self._wait_for_park_completion(telescope)

            return "Alpaca telescope parked successfully."
        finally:
            try:
                telescope.Connected = False
            except Exception:
                pass

    def _unpark_telescope_sync(
        self,
        protocol: Literal["http", "https"] | None,
    ) -> str:
        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        effective_telescope_device_number = int(self.valves.default_telescope_device_number)
        effective_protocol = protocol or self.valves.default_protocol

        try:
            from alpaca.telescope import Telescope
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        telescope = Telescope(endpoint, effective_telescope_device_number, protocol=effective_protocol)

        try:
            telescope.Connected = True

            if not bool(self._safe_getattr(telescope, "AtPark", default=False)):
                return "Alpaca telescope is already unparked."

            can_unpark = self._safe_getattr(
                telescope,
                "CanUnpark",
                default=hasattr(telescope, "Unpark"),
            )
            if can_unpark is False:
                raise RuntimeError("Configured telescope reports CanUnpark=False")
            if not hasattr(telescope, "Unpark"):
                raise RuntimeError("Configured telescope does not expose Unpark()")

            telescope.Unpark()
            self._wait_for_unpark_completion(telescope)

            return "Alpaca telescope unparked successfully."
        finally:
            try:
                telescope.Connected = False
            except Exception:
                pass

    def _run_background_capture_job(self, job_id: str, args: dict[str, object]) -> None:
        try:
            result_text = self._run_slew_and_capture(
                object_name=args.get("object_name"),
                ra_hours=args.get("ra_hours"),
                dec_degrees=args.get("dec_degrees"),
                protocol=args.get("protocol"),
                exposure_seconds=args.get("exposure_seconds"),
                exposure_count=int(args.get("exposure_count", 1)),
                light_frame=bool(args.get("light_frame", True)),
                file_stem=args.get("file_stem"),
                bin_x=int(args.get("bin_x", 1)),
                bin_y=int(args.get("bin_y", 1)),
                enable_tracking=bool(args.get("enable_tracking", True)),
            )
            now = datetime.now(timezone.utc).isoformat()
            with self._JOB_LOCK:
                row = self._JOBS.get(job_id)
                if row is not None:
                    row["status"] = "completed"
                    row["updated_at"] = now
                    row["completed_at"] = now
                    row["result"] = result_text
        except Exception as exc:
            now = datetime.now(timezone.utc).isoformat()
            with self._JOB_LOCK:
                row = self._JOBS.get(job_id)
                if row is not None:
                    row["status"] = "failed"
                    row["updated_at"] = now
                    row["completed_at"] = now
                    row["error"] = str(exc)
                    row["traceback"] = traceback.format_exc(limit=20)

    def _run_slew_and_capture(
        self,
        object_name: str | None,
        ra_hours: float | None,
        dec_degrees: float | None,
        protocol: Literal["http", "https"] | None,
        exposure_seconds: float | None,
        exposure_count: int,
        light_frame: bool,
        file_stem: str | None,
        bin_x: int,
        bin_y: int,
        enable_tracking: bool,
    ) -> str:
        effective_exposure_seconds = (
            float(exposure_seconds)
            if exposure_seconds is not None
            else float(self.valves.default_exposure_seconds)
        )

        if effective_exposure_seconds <= 0:
            raise ValueError("exposure_seconds must be > 0")
        if int(exposure_count) < 1:
            raise ValueError("exposure_count must be >= 1")
        if bin_x < 1 or bin_y < 1:
            raise ValueError("bin_x and bin_y must be >= 1")

        target_ra, target_dec, target_label = self._resolve_target(
            object_name=object_name,
            ra_hours=ra_hours,
            dec_degrees=dec_degrees,
        )

        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        effective_telescope_device_number = int(self.valves.default_telescope_device_number)
        effective_camera_device_number = int(self.valves.default_camera_device_number)

        effective_protocol = protocol or self.valves.default_protocol

        try:
            from alpaca.camera import Camera
            from alpaca.telescope import Telescope
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        telescope = Telescope(endpoint, effective_telescope_device_number, protocol=effective_protocol)
        camera = Camera(endpoint, effective_camera_device_number, protocol=effective_protocol)

        started_at = datetime.now(timezone.utc)
        output_paths: list[Path] = []
        resolved_label = target_label

        try:
            telescope.Connected = True
            camera.Connected = True

            self._ensure_device_connected(telescope, "telescope")
            self._ensure_device_connected(camera, "camera")

            self._ensure_telescope_ready_for_motion(telescope)

            if enable_tracking:
                try:
                    telescope.Tracking = True
                except Exception:
                    # Some mounts may not support toggling Tracking; proceed and let slew fail if needed.
                    pass

            try:
                self._start_slew(telescope=telescope, ra_hours=target_ra, dec_degrees=target_dec)
                self._wait_for_slew_completion(
                    telescope=telescope,
                    target_ra_hours=target_ra,
                    target_dec_degrees=target_dec,
                )
            except Exception as exc:
                diagnostics = self._format_telescope_motion_diagnostics(telescope)
                raise RuntimeError(
                    "Telescope slew failed before capture. "
                    f"Diagnostics: {diagnostics}. "
                    "Check that the mount is unparked, aligned/homed, and accepts remote slew commands. "
                    f"Original error: {exc}"
                ) from exc

            self._configure_camera(camera=camera, bin_x=bin_x, bin_y=bin_y)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            base_stem = file_stem or f"alpaca_{self._sanitize_for_filename(resolved_label)}_{timestamp}"

            for frame_index in range(1, int(exposure_count) + 1):
                camera.StartExposure(float(effective_exposure_seconds), bool(light_frame))
                self._wait_for_image_ready(camera=camera)

                image_data = camera.ImageArray
                image_info = camera.ImageArrayInfo
                exposure_actual = self._safe_getattr(camera, "LastExposureDuration")
                exposure_start = self._safe_getattr(camera, "LastExposureStartTime")
                stem = self._frame_stem(base_stem, frame_index, int(exposure_count))

                output_path = self._save_fits(
                    image_data=image_data,
                    image_info=image_info,
                    camera=camera,
                    file_stem=stem,
                    object_name=object_name,
                    ra_hours=target_ra,
                    dec_degrees=target_dec,
                    exposure_seconds=effective_exposure_seconds,
                    exposure_actual=exposure_actual,
                    exposure_start=exposure_start,
                    started_at=started_at,
                )
                output_paths.append(output_path)

            current_ra = self._safe_getattr(telescope, "RightAscension")
            current_dec = self._safe_getattr(telescope, "Declination")

            return (
                "Alpaca capture completed successfully.\n"
                f"Target: {resolved_label}\n"
                f"Requested coordinates: RA={target_ra:.6f}h, DEC={target_dec:.6f}deg\n"
                f"Telescope reported: RA={current_ra}, DEC={current_dec}\n"
                f"Exposures captured: {int(exposure_count)} x {effective_exposure_seconds}s (light_frame={light_frame})\n"
                "Saved FITS files:\n"
                + "\n".join(self._files_api_markdown_link(path) for path in output_paths)
            )
        finally:
            try:
                camera.Connected = False
            except Exception:
                pass
            try:
                telescope.Connected = False
            except Exception:
                pass

    def _run_slew_and_plate_solve(
        self,
        object_name: str | None,
        ra_hours: float | None,
        dec_degrees: float | None,
        protocol: Literal["http", "https"] | None,
        plate_solve_exposure_seconds: float | None,
        file_stem: str | None,
        bin_x: int,
        bin_y: int,
        enable_tracking: bool,
    ) -> str:
        effective_exposure_seconds = (
            float(plate_solve_exposure_seconds)
            if plate_solve_exposure_seconds is not None
            else float(self.valves.plate_solve_exposure_seconds)
        )

        if effective_exposure_seconds <= 0:
            raise ValueError("plate_solve_exposure_seconds must be > 0")
        if bin_x < 1 or bin_y < 1:
            raise ValueError("bin_x and bin_y must be >= 1")

        target_ra, target_dec, target_label = self._resolve_target(
            object_name=object_name,
            ra_hours=ra_hours,
            dec_degrees=dec_degrees,
        )

        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        effective_telescope_device_number = int(self.valves.default_telescope_device_number)
        effective_camera_device_number = int(self.valves.default_camera_device_number)
        effective_protocol = protocol or self.valves.default_protocol

        try:
            from alpaca.camera import Camera
            from alpaca.telescope import Telescope
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        telescope = Telescope(endpoint, effective_telescope_device_number, protocol=effective_protocol)
        camera = Camera(endpoint, effective_camera_device_number, protocol=effective_protocol)

        started_at = datetime.now(timezone.utc)
        try:
            telescope.Connected = True
            camera.Connected = True

            self._ensure_device_connected(telescope, "telescope")
            self._ensure_device_connected(camera, "camera")

            self._ensure_telescope_ready_for_motion(telescope)

            if enable_tracking:
                try:
                    telescope.Tracking = True
                except Exception:
                    pass

            try:
                self._start_slew(telescope=telescope, ra_hours=target_ra, dec_degrees=target_dec)
                self._wait_for_slew_completion(
                    telescope=telescope,
                    target_ra_hours=target_ra,
                    target_dec_degrees=target_dec,
                )
            except Exception as exc:
                diagnostics = self._format_telescope_motion_diagnostics(telescope)
                raise RuntimeError(
                    "Telescope slew failed before plate solve. "
                    f"Diagnostics: {diagnostics}. "
                    "Check that the mount is unparked, aligned/homed, and accepts remote slew commands. "
                    f"Original error: {exc}"
                ) from exc

            self._configure_camera(camera=camera, bin_x=bin_x, bin_y=bin_y)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            stem = file_stem or f"alpaca_platesolve_{self._sanitize_for_filename(target_label)}_{timestamp}"

            camera.StartExposure(float(effective_exposure_seconds), True)
            self._wait_for_image_ready(camera=camera)

            out_path = self._save_fits(
                image_data=camera.ImageArray,
                image_info=camera.ImageArrayInfo,
                camera=camera,
                file_stem=stem,
                object_name=object_name,
                ra_hours=target_ra,
                dec_degrees=target_dec,
                exposure_seconds=effective_exposure_seconds,
                exposure_actual=self._safe_getattr(camera, "LastExposureDuration"),
                exposure_start=self._safe_getattr(camera, "LastExposureStartTime"),
                started_at=started_at,
            )

            binary = self._resolve_astap_binary()
            if not binary:
                raise RuntimeError(
                    "ASTAP executable not found. Install ASTAP in the container or set astap_binary_path/ASTAP_BINARY_PATH."
                )

            cmd = [binary, "-f", str(out_path.resolve()), "-update", "-wcs"]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=int(self.valves.astap_timeout_seconds),
                check=False,
            )
            combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
            solved = self._astap_looks_solved(combined, proc.returncode)
            solved_ra_hours, solved_dec_degrees = self._extract_astap_solved_coordinates(combined)

            return (
                f"Alpaca slew and plate solve completed for {target_label}.\n"
                f"Requested coordinates: RA={target_ra:.6f}h, DEC={target_dec:.6f}deg\n"
                f"Verification frame: {self._files_api_markdown_link(out_path)}\n"
                f"Plate solved: {solved}\n"
                f"Solved RA (hours): {solved_ra_hours}\n"
                f"Solved DEC (degrees): {solved_dec_degrees}\n"
                f"ASTAP command: {' '.join(shlex.quote(part) for part in cmd)}\n"
                f"ASTAP return code: {proc.returncode}"
            )
        finally:
            try:
                camera.Connected = False
            except Exception:
                pass
            try:
                telescope.Connected = False
            except Exception:
                pass

    def _run_plate_solve_current_position(
        self,
        protocol: Literal["http", "https"] | None,
        plate_solve_exposure_seconds: float | None,
        file_stem: str | None,
        bin_x: int,
        bin_y: int,
    ) -> str:
        effective_exposure_seconds = (
            float(plate_solve_exposure_seconds)
            if plate_solve_exposure_seconds is not None
            else float(self.valves.plate_solve_exposure_seconds)
        )

        if effective_exposure_seconds <= 0:
            raise ValueError("plate_solve_exposure_seconds must be > 0")
        if bin_x < 1 or bin_y < 1:
            raise ValueError("bin_x and bin_y must be >= 1")

        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        effective_telescope_device_number = int(self.valves.default_telescope_device_number)
        effective_camera_device_number = int(self.valves.default_camera_device_number)
        effective_protocol = protocol or self.valves.default_protocol

        try:
            from alpaca.camera import Camera
            from alpaca.telescope import Telescope
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        telescope = Telescope(endpoint, effective_telescope_device_number, protocol=effective_protocol)
        camera = Camera(endpoint, effective_camera_device_number, protocol=effective_protocol)

        started_at = datetime.now(timezone.utc)
        try:
            telescope.Connected = True
            camera.Connected = True

            self._ensure_device_connected(telescope, "telescope")
            self._ensure_device_connected(camera, "camera")

            current_ra = self._safe_getattr(telescope, "RightAscension")
            current_dec = self._safe_getattr(telescope, "Declination")
            try:
                current_ra_hours = float(current_ra)
                current_dec_degrees = float(current_dec)
            except Exception as exc:
                raise RuntimeError("Unable to read current telescope coordinates for plate solving") from exc

            self._configure_camera(camera=camera, bin_x=bin_x, bin_y=bin_y)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            stem = file_stem or f"alpaca_current_platesolve_{timestamp}"

            camera.StartExposure(float(effective_exposure_seconds), True)
            self._wait_for_image_ready(camera=camera)

            out_path = self._save_fits(
                image_data=camera.ImageArray,
                image_info=camera.ImageArrayInfo,
                camera=camera,
                file_stem=stem,
                object_name="current_pointing",
                ra_hours=current_ra_hours,
                dec_degrees=current_dec_degrees,
                exposure_seconds=effective_exposure_seconds,
                exposure_actual=self._safe_getattr(camera, "LastExposureDuration"),
                exposure_start=self._safe_getattr(camera, "LastExposureStartTime"),
                started_at=started_at,
            )

            binary = self._resolve_astap_binary()
            if not binary:
                raise RuntimeError(
                    "ASTAP executable not found. Install ASTAP in the container or set astap_binary_path/ASTAP_BINARY_PATH."
                )

            cmd = [binary, "-f", str(out_path.resolve()), "-update", "-wcs"]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=int(self.valves.astap_timeout_seconds),
                check=False,
            )
            combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
            solved = self._astap_looks_solved(combined, proc.returncode)
            solved_ra_hours, solved_dec_degrees = self._extract_astap_solved_coordinates(combined)

            return (
                "Alpaca plate solve at current location completed.\n"
                f"Telescope current coordinates: RA={current_ra_hours:.6f}h, DEC={current_dec_degrees:.6f}deg\n"
                f"Verification frame: {self._files_api_markdown_link(out_path)}\n"
                f"Plate solved: {solved}\n"
                f"Solved RA (hours): {solved_ra_hours}\n"
                f"Solved DEC (degrees): {solved_dec_degrees}\n"
                f"ASTAP command: {' '.join(shlex.quote(part) for part in cmd)}\n"
                f"ASTAP return code: {proc.returncode}"
            )
        finally:
            try:
                camera.Connected = False
            except Exception:
                pass
            try:
                telescope.Connected = False
            except Exception:
                pass

    def _resolve_target(
        self,
        object_name: str | None,
        ra_hours: float | None,
        dec_degrees: float | None,
    ) -> tuple[float, float, str]:
        if object_name and (ra_hours is not None or dec_degrees is not None):
            raise ValueError("Provide either object_name OR ra_hours+dec_degrees, not both.")

        if object_name:
            cleaned = object_name.strip()
            if not cleaned:
                raise ValueError("object_name cannot be empty")
            ra_h, dec_d, resolved_name = self._resolve_object_coordinates(cleaned)

            display_label = cleaned
            known_name = self._CATALOG_COMMON_NAMES.get(self._normalize_catalog_key(cleaned))
            if known_name:
                display_label = f"{cleaned} ({known_name})"
            elif resolved_name and resolved_name.lower() != cleaned.lower():
                display_label = f"{cleaned} ({resolved_name})"

            return ra_h, dec_d, display_label

        if ra_hours is None or dec_degrees is None:
            raise ValueError("Provide object_name or both ra_hours and dec_degrees.")

        ra_h = float(ra_hours)
        dec_d = float(dec_degrees)

        if not (0.0 <= ra_h <= 24.0):
            raise ValueError("ra_hours must be in the range 0..24")
        if not (-90.0 <= dec_d <= 90.0):
            raise ValueError("dec_degrees must be in the range -90..90")

        return ra_h, dec_d, "coordinate_target"

    def _resolve_object_coordinates(self, object_name: str) -> tuple[float, float, str | None]:
        # Try SIMBAD first because it usually provides robust object aliases.
        try:
            from astroquery.simbad import Simbad
            from astropy.coordinates import SkyCoord
            from astropy import units as u

            simbad = Simbad()
            try:
                simbad.add_votable_fields("ids")
            except Exception:
                pass

            table = simbad.query_object(object_name)
            if table is not None and len(table) > 0:
                ra_text = str(table["RA"][0])
                dec_text = str(table["DEC"][0])
                coord = SkyCoord(ra_text, dec_text, unit=(u.hourangle, u.deg), frame="icrs")

                resolved_name = None
                if "IDS" in table.colnames:
                    resolved_name = self._extract_preferred_name_from_ids(str(table["IDS"][0]))
                if not resolved_name and "MAIN_ID" in table.colnames:
                    resolved_name = str(table["MAIN_ID"][0]).strip()

                return float(coord.ra.hour), float(coord.dec.deg), resolved_name
        except Exception:
            pass

        # Fallback to astropy's general name resolver.
        try:
            from astropy.coordinates import SkyCoord

            coord = SkyCoord.from_name(object_name)
            return float(coord.ra.hour), float(coord.dec.deg), None
        except Exception as exc:
            raise ValueError(f"Unable to resolve object name '{object_name}' to RA/DEC") from exc

    @staticmethod
    def _extract_preferred_name_from_ids(ids_text: str) -> str | None:
        # Prefer SIMBAD NAME aliases when available.
        for token in [part.strip() for part in str(ids_text or "").split("|") if part.strip()]:
            if token.upper().startswith("NAME "):
                value = token[5:].strip()
                return value or None
        return None

    @staticmethod
    def _normalize_catalog_key(value: str) -> str:
        # Normalize forms such as "M 45" and "m45" into "M45".
        compact = re.sub(r"\s+", "", str(value or "")).upper()
        if compact.startswith("M") and compact[1:].isdigit():
            return f"M{int(compact[1:])}"
        return compact

    def _start_slew(self, telescope, ra_hours: float, dec_degrees: float) -> None:
        can_slew_async = bool(self._safe_getattr(telescope, "CanSlewAsync", default=True))
        can_slew_sync = bool(self._safe_getattr(telescope, "CanSlew", default=True))
        async_error: Exception | None = None

        if not can_slew_async and not can_slew_sync:
            raise RuntimeError("Configured telescope reports CanSlew=False and CanSlewAsync=False")

        if can_slew_async and hasattr(telescope, "SlewToCoordinatesAsync"):
            try:
                telescope.SlewToCoordinatesAsync(ra_hours, dec_degrees)
                # If async call succeeds, stay on async path. Some Alpaca mounts
                # report Slewing=False even while a move is in progress and reject
                # synchronous methods entirely.
                return
            except Exception as exc:
                # Some mounts reject async slews (for example 0x4ff) but accept sync slews.
                async_error = exc

        if not can_slew_sync:
            if async_error is not None:
                raise RuntimeError(
                    "Configured telescope reports CanSlew=False and async slew failed: "
                    f"{async_error}"
                ) from async_error
            raise RuntimeError("Configured telescope reports CanSlew=False")

        telescope.SlewToCoordinates(ra_hours, dec_degrees)

    def _format_telescope_motion_diagnostics(self, telescope) -> str:
        fields = [
            "Connected",
            "AtPark",
            "CanUnpark",
            "CanSlew",
            "CanSlewAsync",
            "Tracking",
            "Slewing",
            "RightAscension",
            "Declination",
        ]
        parts: list[str] = []
        for field in fields:
            value = self._safe_getattr(telescope, field, default="unknown")
            parts.append(f"{field}={value}")
        return ", ".join(parts)

    def _wait_for_slew_completion(
        self,
        telescope,
        target_ra_hours: float | None = None,
        target_dec_degrees: float | None = None,
    ) -> None:
        deadline = time.monotonic() + float(self.valves.slew_timeout_seconds)
        poll = float(self.valves.slew_poll_seconds)
        use_target_convergence = target_ra_hours is not None and target_dec_degrees is not None

        while time.monotonic() < deadline:
            if use_target_convergence and self._telescope_at_target(telescope, target_ra_hours, target_dec_degrees):
                return

            try:
                slewing = bool(telescope.Slewing)
                if not slewing and not use_target_convergence:
                    return
            except Exception:
                # Some drivers don't expose reliable Slewing.
                if use_target_convergence and self._telescope_at_target(telescope, target_ra_hours, target_dec_degrees):
                    return
            time.sleep(poll)
        raise TimeoutError("Timed out waiting for telescope slew to complete")

    def _wait_for_slew_start(self, telescope, timeout_seconds: float = 5.0) -> bool:
        deadline = time.monotonic() + float(timeout_seconds)
        while time.monotonic() < deadline:
            try:
                if bool(telescope.Slewing):
                    return True
            except Exception:
                return False
            time.sleep(0.2)
        return False

    def _telescope_at_target(self, telescope, ra_hours: float, dec_degrees: float) -> bool:
        current_ra = self._safe_getattr(telescope, "RightAscension")
        current_dec = self._safe_getattr(telescope, "Declination")
        try:
            ra_cur = float(current_ra)
            dec_cur = float(current_dec)
        except Exception:
            return False

        # RA wraps at 24h; use shortest wrapped distance.
        ra_delta = abs(ra_cur - float(ra_hours))
        ra_delta = min(ra_delta, 24.0 - ra_delta)
        dec_delta = abs(dec_cur - float(dec_degrees))
        return ra_delta <= 0.03 and dec_delta <= 0.5

    def _wait_for_park_completion(self, telescope) -> None:
        deadline = time.monotonic() + float(self.valves.slew_timeout_seconds)
        poll = float(self.valves.slew_poll_seconds)
        while time.monotonic() < deadline:
            try:
                if bool(telescope.AtPark):
                    return
            except Exception:
                # If AtPark is unavailable, assume Park() call completed synchronously.
                return
            time.sleep(poll)
        raise TimeoutError("Timed out waiting for telescope park completion")

    def _wait_for_unpark_completion(self, telescope) -> None:
        deadline = time.monotonic() + float(self.valves.slew_timeout_seconds)
        poll = float(self.valves.slew_poll_seconds)
        while time.monotonic() < deadline:
            try:
                if not bool(telescope.AtPark):
                    return
            except Exception:
                # If AtPark is unavailable, assume Unpark() call completed synchronously.
                return
            time.sleep(poll)
        raise TimeoutError("Timed out waiting for telescope unpark completion")

    def _ensure_telescope_ready_for_motion(self, telescope) -> None:
        if not bool(self._safe_getattr(telescope, "AtPark", default=False)):
            return

        can_unpark = self._safe_getattr(
            telescope,
            "CanUnpark",
            default=hasattr(telescope, "Unpark"),
        )
        if can_unpark is False:
            raise RuntimeError("Telescope is parked and reports CanUnpark=False")
        if not hasattr(telescope, "Unpark"):
            raise RuntimeError("Telescope is parked but Unpark() is unavailable")

        telescope.Unpark()
        self._wait_for_unpark_completion(telescope)

    def _ensure_device_connected(self, device, device_label: str) -> None:
        state = self._safe_getattr(device, "Connected", default=None)
        if state is None:
            # Some drivers may not expose a readable property; assume success when writable.
            return
        if not bool(state):
            raise RuntimeError(f"Configured Alpaca {device_label} is not connected")

    def _resolve_astap_binary(self) -> str | None:
        candidates: list[str] = []
        configured = str(getattr(self.valves, "astap_binary_path", "") or "").strip()
        if configured:
            candidates.append(configured)

        env_override = str(os.environ.get("ASTAP_BINARY_PATH", "") or "").strip()
        if env_override:
            candidates.append(env_override)

        candidates.extend(self._DEFAULT_ASTAP_BINARY_CANDIDATES)

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

    @staticmethod
    def _astap_looks_solved(output_text: str, return_code: int) -> bool:
        text = str(output_text or "").lower()
        if "no solution" in text or "failed" in text:
            return False
        if "solution" in text or "solved" in text or "wcs" in text:
            return True
        return return_code == 0

    @staticmethod
    def _extract_astap_solved_coordinates(output_text: str) -> tuple[float | None, float | None]:
        text = str(output_text or "")

        ra_match = re.search(r"\bRA\s*[:=]\s*([0-9]{1,2}(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
        dec_match = re.search(r"\bDEC\s*[:=]\s*([+-]?[0-9]{1,2}(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)

        ra_val = float(ra_match.group(1)) if ra_match else None
        dec_val = float(dec_match.group(1)) if dec_match else None
        return ra_val, dec_val

    def _configure_camera(self, camera, bin_x: int, bin_y: int) -> None:
        camera.BinX = int(bin_x)
        camera.BinY = int(bin_y)
        camera.StartX = 0
        camera.StartY = 0
        camera.NumX = int(camera.CameraXSize // camera.BinX)
        camera.NumY = int(camera.CameraYSize // camera.BinY)

    def _wait_for_image_ready(self, camera) -> None:
        deadline = time.monotonic() + float(self.valves.exposure_timeout_seconds)
        poll = float(self.valves.exposure_poll_seconds)
        while time.monotonic() < deadline:
            if bool(camera.ImageReady):
                return
            time.sleep(poll)
        raise TimeoutError("Timed out waiting for camera image readiness")

    def _save_fits(
        self,
        image_data,
        image_info,
        camera,
        file_stem: str,
        object_name: str | None,
        ra_hours: float,
        dec_degrees: float,
        exposure_seconds: float,
        exposure_actual,
        exposure_start,
        started_at: datetime,
    ) -> Path:
        import numpy as np
        from astropy.io import fits

        safe_stem = self._sanitize_for_filename(file_stem)
        out_dir = self._OUTPUT_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{safe_stem}.fits"

        image_array = self._normalize_image_array(np.array(image_data), image_info)

        header = fits.Header()
        header["DATE"] = datetime.now(timezone.utc).isoformat()
        header["DATE-OBS"] = str(exposure_start or started_at.isoformat())
        header["EXPTIME"] = float(exposure_actual if exposure_actual is not None else exposure_seconds)
        header["OBJNAME"] = str(object_name or "coordinate_target")
        header["RA_HRS"] = float(ra_hours)
        header["DEC_DEG"] = float(dec_degrees)
        self._populate_camera_header(header=header, camera=camera, image_array=image_array)
        header["HISTORY"] = "Captured via AstroLlama alpaca_slew_and_capture tool"

        hdu = fits.PrimaryHDU(image_array, header=header)
        hdu.writeto(out_path, overwrite=True)
        return out_path

    @staticmethod
    def _sanitize_for_filename(value: str) -> str:
        cleaned = re.sub(r"\s+", "_", str(value or "capture").strip())
        cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", cleaned)
        cleaned = cleaned.strip("._")
        return cleaned or "capture"

    @staticmethod
    def _frame_stem(base_stem: str, frame_index: int, exposure_count: int) -> str:
        if int(exposure_count) <= 1:
            return base_stem
        width = max(3, len(str(int(exposure_count))))
        return f"{base_stem}_{int(frame_index):0{width}d}"

    @staticmethod
    def _files_api_markdown_link(path: Path) -> str:
        filename = path.name
        url = f"/api/files/{quote(filename)}"
        return f"[{filename}]({url})"

    @staticmethod
    def _sensor_type_name(sensor_type_value: object) -> str | None:
        mapping = {
            0: "Monochrome",
            1: "Color",
            2: "RGGB",
            3: "CMYG",
            4: "CMYG2",
            5: "LRGB",
        }
        try:
            return mapping.get(int(sensor_type_value))
        except Exception:
            return None

    @staticmethod
    def _normalize_image_array(image_array, image_info):
        rank = int(getattr(image_info, "Rank", image_array.ndim) or image_array.ndim)

        if rank <= 2 or image_array.ndim <= 2:
            return image_array.transpose()

        if image_array.ndim != 3:
            return image_array

        dim1 = int(getattr(image_info, "Dimension1", 0) or 0)
        dim2 = int(getattr(image_info, "Dimension2", 0) or 0)
        dim3 = int(getattr(image_info, "Dimension3", 0) or 0)
        sx, sy, sz = image_array.shape

        # Most Alpaca drivers expose color as [plane, x, y]; convert to [y, x, plane].
        if sx in (3, 4) and (dim3 in (0, sx, 3, 4) or (dim1 == sy and dim2 == sz)):
            return image_array.transpose(2, 1, 0)

        # Alternate form [x, y, plane] -> [y, x, plane].
        if sz in (3, 4) and (dim3 in (0, sz, 3, 4) or (dim1 == sx and dim2 == sy)):
            return image_array.transpose(1, 0, 2)

        return image_array.transpose(2, 1, 0)

    def _populate_camera_header(self, header, camera, image_array) -> None:
        sensor_type_value = self._safe_getattr(camera, "SensorType")
        sensor_type_name = self._sensor_type_name(sensor_type_value)

        if sensor_type_value is not None:
            try:
                header["SENSORTY"] = int(sensor_type_value)
            except Exception:
                header["SENSORTY"] = str(sensor_type_value)
        if sensor_type_name:
            header["CAMSENS"] = sensor_type_name

        if image_array.ndim == 3 and int(image_array.shape[-1]) in (3, 4):
            header["COLORTYP"] = "RGB" if int(image_array.shape[-1]) == 3 else "RGBA"
            header["CTYPE3"] = "RGB"

        # Bayer metadata helps downstream viewers/processors interpret raw color sensors.
        bayer_pattern = sensor_type_name if sensor_type_name in {"RGGB", "CMYG", "CMYG2", "LRGB"} else None
        if bayer_pattern:
            header["BAYERPAT"] = bayer_pattern
            bx = self._safe_getattr(camera, "BayerOffsetX")
            by = self._safe_getattr(camera, "BayerOffsetY")
            if bx is not None:
                header["XBAYROFF"] = int(bx)
            if by is not None:
                header["YBAYROFF"] = int(by)

    def _collect_camera_profile(self, camera) -> dict[str, object]:
        names = [
            "Name",
            "Description",
            "DriverInfo",
            "DriverVersion",
            "InterfaceVersion",
            "SensorType",
            "BayerOffsetX",
            "BayerOffsetY",
            "CameraXSize",
            "CameraYSize",
            "MaxBinX",
            "MaxBinY",
            "CanAsymmetricBin",
            "CanAbortExposure",
            "CanStopExposure",
            "HasShutter",
            "PixelSizeX",
            "PixelSizeY",
            "ElectronsPerADU",
            "FullWellCapacity",
            "ReadoutModes",
            "Gain",
            "Offset",
            "CCDTemperature",
            "CoolerOn",
            "CanSetCCDTemperature",
            "SetCCDTemperature",
        ]

        data: dict[str, object] = {}
        for name in names:
            value = self._safe_getattr(camera, name)
            if value is not None:
                data[name] = value

        sensor_type_name = self._sensor_type_name(data.get("SensorType"))
        if sensor_type_name:
            data["SensorTypeName"] = sensor_type_name

        fits_hints: dict[str, object] = {}
        if "SensorType" in data:
            fits_hints["SENSORTY"] = data["SensorType"]
        if sensor_type_name:
            fits_hints["CAMSENS"] = sensor_type_name
        if sensor_type_name in {"RGGB", "CMYG", "CMYG2", "LRGB"}:
            fits_hints["BAYERPAT"] = sensor_type_name
            if "BayerOffsetX" in data:
                fits_hints["XBAYROFF"] = data["BayerOffsetX"]
            if "BayerOffsetY" in data:
                fits_hints["YBAYROFF"] = data["BayerOffsetY"]

        if fits_hints:
            data["FitsHeaderHints"] = fits_hints

        return data

    @staticmethod
    def _safe_getattr(obj, name: str, default=None):
        try:
            return getattr(obj, name)
        except Exception:
            return default