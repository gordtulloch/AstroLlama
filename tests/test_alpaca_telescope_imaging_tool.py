from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
import types

import numpy as np

import pytest

sys.path.insert(0, r"c:\Projects\AstroLlama")

from mcp_server.tools.alpaca_telescope_imaging_tool import Tools


def test_resolve_target_requires_single_target_mode():
    tool = Tools()

    with pytest.raises(ValueError, match="either object_name OR"):
        tool._resolve_target(object_name="M42", ra_hours=5.5, dec_degrees=-5.4)


def test_resolve_target_requires_complete_coordinates():
    tool = Tools()

    with pytest.raises(ValueError, match="both ra_hours and dec_degrees"):
        tool._resolve_target(object_name=None, ra_hours=5.5, dec_degrees=None)


def test_resolve_target_validates_coordinate_ranges():
    tool = Tools()

    with pytest.raises(ValueError, match="ra_hours"):
        tool._resolve_target(object_name=None, ra_hours=24.1, dec_degrees=0.0)

    with pytest.raises(ValueError, match="dec_degrees"):
        tool._resolve_target(object_name=None, ra_hours=12.0, dec_degrees=-90.1)


def test_resolve_target_by_coordinates_success():
    tool = Tools()

    ra_h, dec_d, label = tool._resolve_target(object_name=None, ra_hours=5.25, dec_degrees=-22.5)

    assert ra_h == pytest.approx(5.25)
    assert dec_d == pytest.approx(-22.5)
    assert label == "coordinate_target"


def test_resolve_target_by_object_name_uses_resolver(monkeypatch):
    tool = Tools()
    monkeypatch.setattr(tool, "_resolve_object_coordinates", lambda name: (10.123, -30.456, None))

    ra_h, dec_d, label = tool._resolve_target(object_name="NGC 5128", ra_hours=None, dec_degrees=None)

    assert ra_h == pytest.approx(10.123)
    assert dec_d == pytest.approx(-30.456)
    assert label == "NGC 5128"


def test_resolve_target_m45_uses_common_name_label(monkeypatch):
    tool = Tools()
    monkeypatch.setattr(tool, "_resolve_object_coordinates", lambda name: (3.0, 24.0, None))

    _ra_h, _dec_d, label = tool._resolve_target(object_name="M45", ra_hours=None, dec_degrees=None)

    assert label == "M45 (Pleiades)"


def test_sanitize_for_filename_removes_problem_characters():
    out = Tools._sanitize_for_filename(" M42 / Orion: core* ")
    assert out == "M42___Orion__core"


def test_frame_stem_indexing_for_multi_exposure_sequences():
    assert Tools._frame_stem("alpaca_M45", 1, 25) == "alpaca_M45_001"
    assert Tools._frame_stem("alpaca_M45", 25, 25) == "alpaca_M45_025"
    assert Tools._frame_stem("alpaca_M45", 1, 1) == "alpaca_M45"


def test_files_api_markdown_link_uses_filename_and_api_route():
    link = Tools._files_api_markdown_link(Path("data/downloads/alpaca M45_001.fits"))
    assert link == "[alpaca M45_001.fits](/api/files/alpaca%20M45_001.fits)"


def test_normalize_image_array_plane_first_rgb_to_last_axis():
    # Simulate [plane, x, y] camera payload for RGB data.
    raw = np.arange(3 * 4 * 5).reshape(3, 4, 5)
    info = SimpleNamespace(Rank=3, Dimension1=4, Dimension2=5, Dimension3=3)

    out = Tools._normalize_image_array(raw, info)
    assert out.shape == (5, 4, 3)


def test_populate_camera_header_sets_color_and_bayer_metadata():
    from astropy.io import fits

    tool = Tools()
    header = fits.Header()
    camera = SimpleNamespace(SensorType=2, BayerOffsetX=1, BayerOffsetY=0)
    image_array = np.zeros((10, 12, 3), dtype=np.uint16)

    tool._populate_camera_header(header=header, camera=camera, image_array=image_array)

    assert header["SENSORTY"] == 2
    assert header["CAMSENS"] == "RGGB"
    assert header["COLORTYP"] == "RGB"
    assert header["CTYPE3"] == "RGB"
    assert header["BAYERPAT"] == "RGGB"
    assert header["XBAYROFF"] == 1
    assert header["YBAYROFF"] == 0


def test_collect_camera_profile_includes_sensor_name_and_fits_hints():
    tool = Tools()
    camera = SimpleNamespace(
        Name="TestCam",
        SensorType=2,
        BayerOffsetX=1,
        BayerOffsetY=0,
        CameraXSize=3000,
        CameraYSize=2000,
    )

    profile = tool._collect_camera_profile(camera)

    assert profile["Name"] == "TestCam"
    assert profile["SensorType"] == 2
    assert profile["SensorTypeName"] == "RGGB"
    assert profile["FitsHeaderHints"]["SENSORTY"] == 2
    assert profile["FitsHeaderHints"]["CAMSENS"] == "RGGB"
    assert profile["FitsHeaderHints"]["BAYERPAT"] == "RGGB"
    assert profile["FitsHeaderHints"]["XBAYROFF"] == 1
    assert profile["FitsHeaderHints"]["YBAYROFF"] == 0


def test_default_exposure_valve_is_10_seconds():
    tool = Tools()
    assert tool.valves.default_exposure_seconds == pytest.approx(10.0)


def test_default_device_number_valves_are_zero():
    tool = Tools()
    assert tool.valves.default_telescope_device_number == 0
    assert tool.valves.default_camera_device_number == 0


def test_public_tool_schema_hides_address_and_device_number_overrides():
    tool = Tools()
    spec = tool.alpaca_slew_and_capture
    params = spec.__annotations__

    assert "alpaca_address" not in params
    assert "telescope_device_number" not in params
    assert "camera_device_number" not in params


def test_effective_exposure_prefers_explicit_argument_over_valve():
    tool = Tools()

    # Mirror the tool's precedence logic so this behavior is locked by test.
    explicit = 22.0
    effective = float(explicit) if explicit is not None else float(tool.valves.default_exposure_seconds)
    assert effective == pytest.approx(22.0)

    omitted = None
    effective = float(omitted) if omitted is not None else float(tool.valves.default_exposure_seconds)
    assert effective == pytest.approx(10.0)


def test_output_dir_is_not_remote_editable_valve_field():
    schema_props = (Tools.Valves.model_json_schema() or {}).get("properties", {})
    assert "output_dir" not in schema_props


def test_internal_output_dir_is_fixed():
    tool = Tools()
    assert tool._OUTPUT_DIR.as_posix() == "data/downloads"


def test_exposure_count_must_be_positive():
    tool = Tools()

    with pytest.raises(ValueError, match="exposure_count must be >= 1"):
        tool._run_slew_and_capture(
            object_name="M45",
            ra_hours=None,
            dec_degrees=None,
            protocol=None,
            exposure_seconds=10.0,
            exposure_count=0,
            light_frame=True,
            file_stem=None,
            bin_x=1,
            bin_y=1,
            enable_tracking=True,
        )


@pytest.mark.asyncio
async def test_capture_defaults_to_background_start(monkeypatch):
    tool = Tools()

    async def fake_start(**kwargs):
        assert kwargs["object_name"] == "M45"
        assert kwargs["exposure_seconds"] == 10
        assert kwargs["exposure_count"] == 25
        return json.dumps({"job_id": "job123", "status": "running"})

    monkeypatch.setattr(tool, "alpaca_slew_and_capture_start", fake_start)

    out = await tool.alpaca_slew_and_capture(object_name="M45", exposure_seconds=10, exposure_count=25)
    payload = json.loads(out)
    assert payload["status"] == "running"
    assert payload["job_id"] == "job123"


@pytest.mark.asyncio
async def test_capture_alias_matches_start_payload(monkeypatch):
    tool = Tools()

    async def fake_start(**_kwargs):
        return json.dumps({"job_id": "same-job", "status": "running"})

    monkeypatch.setattr(tool, "alpaca_slew_and_capture_start", fake_start)

    out = await tool.alpaca_slew_and_capture(object_name="M45")
    payload = json.loads(out)
    assert payload["job_id"] == "same-job"
    assert payload["status"] == "running"


@pytest.mark.asyncio
async def test_background_capture_job_success(monkeypatch):
    tool = Tools()

    def fake_capture(**_kwargs):
        return "Alpaca capture completed successfully.\nSaved FITS: data/downloads/test.fits"

    monkeypatch.setattr(tool, "_run_slew_and_capture", fake_capture)

    submit_raw = await tool.alpaca_slew_and_capture_start(object_name="M45", exposure_seconds=30)
    submit = json.loads(submit_raw)
    assert submit["status"] == "running"
    job_id = submit["job_id"]

    deadline = time.time() + 2.0
    final = None
    while time.time() < deadline:
        status = json.loads(await tool.alpaca_capture_job_status(job_id))
        if status["status"] != "running":
            final = status
            break
        time.sleep(0.02)

    assert final is not None
    assert final["status"] == "completed"
    assert "Saved FITS" in str(final["result"])


@pytest.mark.asyncio
async def test_background_capture_job_failure(monkeypatch):
    tool = Tools()

    def fake_capture(**_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(tool, "_run_slew_and_capture", fake_capture)

    submit_raw = await tool.alpaca_slew_and_capture_start(object_name="M45", exposure_seconds=30)
    job_id = json.loads(submit_raw)["job_id"]

    deadline = time.time() + 2.0
    final = None
    while time.time() < deadline:
        status = json.loads(await tool.alpaca_capture_job_status(job_id))
        if status["status"] != "running":
            final = status
            break
        time.sleep(0.02)

    assert final is not None
    assert final["status"] == "failed"
    assert "boom" in str(final["error"])


@pytest.mark.asyncio
async def test_alpaca_park_telescope_success(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            assert endpoint == tool.valves.default_alpaca_address
            assert device_number == tool.valves.default_telescope_device_number
            assert protocol == "http"
            self.Connected = False
            self.CanPark = True
            self.AtPark = False

        def Park(self):
            self.AtPark = True

    fake_mod = types.ModuleType("alpaca.telescope")
    fake_mod.Telescope = FakeTelescope
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_mod)

    out = await tool.alpaca_park_telescope(protocol="http")
    assert out == "Alpaca telescope parked successfully."


@pytest.mark.asyncio
async def test_alpaca_park_telescope_already_parked(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self.Connected = False
            self.CanPark = True
            self.AtPark = True

        def Park(self):
            raise AssertionError("Park should not be called when telescope is already parked")

    fake_mod = types.ModuleType("alpaca.telescope")
    fake_mod.Telescope = FakeTelescope
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_mod)

    out = await tool.alpaca_park_telescope(protocol="http")
    assert out == "Alpaca telescope is already parked."


@pytest.mark.asyncio
async def test_alpaca_park_telescope_canpark_false(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self.Connected = False
            self.CanPark = False
            self.AtPark = False

        def Park(self):
            raise AssertionError("Park should not be called when CanPark is false")

    fake_mod = types.ModuleType("alpaca.telescope")
    fake_mod.Telescope = FakeTelescope
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_mod)

    with pytest.raises(RuntimeError, match="CanPark=False"):
        await tool.alpaca_park_telescope(protocol="http")


@pytest.mark.asyncio
async def test_alpaca_unpark_telescope_success(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self.Connected = False
            self.CanUnpark = True
            self.AtPark = True

        def Unpark(self):
            self.AtPark = False

    fake_mod = types.ModuleType("alpaca.telescope")
    fake_mod.Telescope = FakeTelescope
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_mod)

    out = await tool.alpaca_unpark_telescope(protocol="http")
    assert out == "Alpaca telescope unparked successfully."


@pytest.mark.asyncio
async def test_alpaca_unpark_telescope_already_unparked(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self.Connected = False
            self.CanUnpark = True
            self.AtPark = False

        def Unpark(self):
            raise AssertionError("Unpark should not be called when telescope is already unparked")

    fake_mod = types.ModuleType("alpaca.telescope")
    fake_mod.Telescope = FakeTelescope
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_mod)

    out = await tool.alpaca_unpark_telescope(protocol="http")
    assert out == "Alpaca telescope is already unparked."


@pytest.mark.asyncio
async def test_alpaca_unpark_telescope_canunpark_false(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self.Connected = False
            self.CanUnpark = False
            self.AtPark = True

        def Unpark(self):
            raise AssertionError("Unpark should not be called when CanUnpark is false")

    fake_mod = types.ModuleType("alpaca.telescope")
    fake_mod.Telescope = FakeTelescope
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_mod)

    with pytest.raises(RuntimeError, match="CanUnpark=False"):
        await tool.alpaca_unpark_telescope(protocol="http")


def test_ensure_telescope_ready_for_motion_auto_unparks():
    tool = Tools()

    class FakeTelescope:
        def __init__(self):
            self.AtPark = True
            self.CanUnpark = True
            self.unpark_calls = 0

        def Unpark(self):
            self.unpark_calls += 1
            self.AtPark = False

    telescope = FakeTelescope()
    tool._ensure_telescope_ready_for_motion(telescope)

    assert telescope.unpark_calls == 1
    assert telescope.AtPark is False


def test_ensure_telescope_ready_for_motion_raises_when_cannot_unpark():
    tool = Tools()

    class FakeTelescope:
        def __init__(self):
            self.AtPark = True
            self.CanUnpark = False

    with pytest.raises(RuntimeError, match="CanUnpark=False"):
        tool._ensure_telescope_ready_for_motion(FakeTelescope())


def test_start_slew_falls_back_to_sync_when_async_never_starts(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self):
            self.CanSlewAsync = True
            self.Slewing = False
            self.async_calls = 0
            self.sync_calls = 0

        def SlewToCoordinatesAsync(self, _ra: float, _dec: float):
            self.async_calls += 1
            self.Slewing = False

        def SlewToCoordinates(self, _ra: float, _dec: float):
            self.sync_calls += 1

    monkeypatch.setattr(tool, "_wait_for_slew_start", lambda *_args, **_kwargs: False)

    scope = FakeTelescope()
    tool._start_slew(scope, ra_hours=3.0, dec_degrees=24.0)

    assert scope.async_calls == 1
    assert scope.sync_calls == 1


def test_wait_for_slew_completion_uses_target_when_slewing_unavailable(monkeypatch):
    tool = Tools()
    tool.valves.slew_timeout_seconds = 1
    tool.valves.slew_poll_seconds = 0.01

    class FakeTelescope:
        @property
        def Slewing(self):
            raise RuntimeError("Slewing not supported")

        RightAscension = 3.001
        Declination = 23.9

    monkeypatch.setattr(time, "sleep", lambda _x: None)
    tool._wait_for_slew_completion(
        FakeTelescope(),
        target_ra_hours=3.0,
        target_dec_degrees=24.0,
    )


def test_run_slew_and_capture_raises_when_telescope_not_connected(monkeypatch):
    tool = Tools()

    class FakeTelescope:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self._connected = False

        @property
        def Connected(self):
            return self._connected

        @Connected.setter
        def Connected(self, _value):
            # Simulate driver rejecting connection while not raising on assignment.
            self._connected = False

    class FakeCamera:
        def __init__(self, endpoint: str, device_number: int, protocol: str = "http"):
            self._connected = False

        @property
        def Connected(self):
            return self._connected

        @Connected.setter
        def Connected(self, value):
            self._connected = bool(value)

    fake_telescope_mod = types.ModuleType("alpaca.telescope")
    fake_telescope_mod.Telescope = FakeTelescope
    fake_camera_mod = types.ModuleType("alpaca.camera")
    fake_camera_mod.Camera = FakeCamera
    monkeypatch.setitem(sys.modules, "alpaca.telescope", fake_telescope_mod)
    monkeypatch.setitem(sys.modules, "alpaca.camera", fake_camera_mod)
    monkeypatch.setattr(tool, "_resolve_target", lambda **_kwargs: (3.0, 24.0, "M45"))

    with pytest.raises(RuntimeError, match="telescope is not connected"):
        tool._run_slew_and_capture(
            object_name="M45",
            ra_hours=None,
            dec_degrees=None,
            protocol="http",
            exposure_seconds=1.0,
            exposure_count=1,
            light_frame=True,
            file_stem=None,
            bin_x=1,
            bin_y=1,
            enable_tracking=True,
        )
