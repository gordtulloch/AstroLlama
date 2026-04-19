"""
Live integration tests for simbad_search.

These tests make real network requests to SIMBAD.
Run with: pytest tests/test_simbad_search_live.py -v

Skip with: pytest -m "not live"
"""
import asyncio
import sys
import pytest
import pytest_asyncio

sys.path.insert(0, r"c:\Projects\AstroLlama")

pytestmark = pytest.mark.live


from mcp_server.data_sources.simbad_search import simbad_search


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Test 1: Brightest stars
# ---------------------------------------------------------------------------

class TestBrightestStars:

    @pytest.fixture(scope="class")
    def result(self):
        return run(simbad_search("List the 10 brightest stars in the sky", limit=10))

    def test_no_error(self, result):
        assert not result.startswith("Sorry"), result[:200]

    def test_has_title(self, result):
        assert "Brightest Stars" in result

    def test_sirius_present(self, result):
        assert "Sirius" in result

    def test_canopus_present(self, result):
        assert "Canopus" in result

    def test_arcturus_present(self, result):
        assert "Arcturus" in result

    def test_rigel_present(self, result):
        assert "Rigel" in result

    def test_betelgeuse_present(self, result):
        assert "Betelgeuse" in result

    def test_has_ten_entries(self, result):
        lines = [l for l in result.splitlines() if l.strip().startswith(("1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.", "10."))]
        assert len(lines) == 10, f"Expected 10 entries, found {len(lines)}"

    def test_types_are_friendly(self, result):
        # None of the raw otype codes should appear as type labels
        bad_patterns = ["Type: *\n", "Type: **\n", "Type: SB*\n", "Type: sg*\n"]
        for pat in bad_patterns:
            assert pat not in result, f"Raw otype appeared: {pat!r}"

    def test_has_simbad_source(self, result):
        assert "SIMBAD" in result

    def test_sirius_is_first(self, result):
        # Sirius (mag -1.46) must be #1
        lines = result.splitlines()
        first_entry = next((l for l in lines if l.strip().startswith("1.")), "")
        assert "Sirius" in first_entry, f"Expected Sirius at #1, got: {first_entry}"


# ---------------------------------------------------------------------------
# Test 2: Emission nebulae in Orion
# ---------------------------------------------------------------------------

class TestOrionEmissionNebulae:

    @pytest.fixture(scope="class")
    def result(self):
        return run(simbad_search("List the emission nebulae in the Orion constellation", limit=10))

    def test_no_error(self, result):
        assert not result.startswith("Sorry"), result[:200]

    def test_has_title(self, result):
        assert "Orion" in result

    def test_friendly_type_label(self, result):
        assert "Emission Nebula (HII Region)" in result

    def test_barnard_loop_present(self, result):
        assert "Barnard" in result

    def test_great_orion_nebula_present(self, result):
        # M42 / Orion Nebula must appear
        assert "Orion Nebula" in result or "Great Orion Nebula" in result or "M42" in result

    def test_rosette_nebula_present(self, result):
        assert "Rosette" in result

    def test_no_raw_hii_type(self, result):
        assert "Type: HII" not in result


# ---------------------------------------------------------------------------
# Test 3: Brightest galaxies in Ursa Major
# ---------------------------------------------------------------------------

class TestUrsaMajorGalaxies:

    @pytest.fixture(scope="class")
    def result(self):
        return run(simbad_search("10 brightest galaxies in Ursa Major", limit=10))

    def test_no_error(self, result):
        assert not result.startswith("Sorry"), result[:200]

    def test_has_title(self, result):
        assert "Ursa Major" in result

    def test_m81_present_and_first(self, result):
        lines = result.splitlines()
        first_entry = next((l for l in lines if l.strip().startswith("1.")), "")
        assert "M81" in first_entry, f"M81 should be #1, got: {first_entry}"

    def test_m82_present(self, result):
        assert "M82" in result

    def test_m82_not_uma_a(self, result):
        assert "UMa A" not in result

    def test_m106_present(self, result):
        assert "M106" in result

    def test_no_raw_otype_codes(self, result):
        # None of these raw codes should appear after "Type: "
        for raw_code in ["LIN\n", "EmG\n", "SBG\n", "GiP\n", "GiF\n", "AGN\n"]:
            assert f"Type: {raw_code}" not in result, f"Raw otype appeared: {raw_code!r}"

    def test_m81_magnitude_correct(self, result):
        # M81 is magnitude ~6.9 — should appear in the brightness description
        assert "6.9" in result

    def test_has_ten_entries(self, result):
        count = sum(1 for l in result.splitlines()
                    if re.match(r"^\d+\.", l.strip()))
        assert count == 10, f"Expected 10 entries, found {count}"


import re
