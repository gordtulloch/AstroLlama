"""
Unit tests for mcp_server.data_sources.simbad_search.

These tests cover the pure-Python functions that require no network access:
  - _friendly_otype
  - _pick_common_name
  - _mag_description
  - _safe_str
  - _parse_natural_language
  - _format_rows (via mock rows)

Network-dependent functions (_query_brightest_stars etc.) and the public
simbad_search() coroutine are tested in tests/test_simbad_search_live.py.
"""
import sys
import re
import pytest

sys.path.insert(0, r"c:\Projects\AstroLlama")

from mcp_server.data_sources.simbad_search import (
    _friendly_otype,
    _pick_common_name,
    _mag_description,
    _safe_str,
    _parse_natural_language,
    _format_rows,
    _OTYPE_LABELS,
    _OTYPE_GROUPS,
    _COMMON_NAMES,
)


# ---------------------------------------------------------------------------
# _friendly_otype
# ---------------------------------------------------------------------------

class TestFriendlyOtype:

    def test_known_star_types(self):
        assert _friendly_otype("*") == "Star"
        assert _friendly_otype("**") == "Double Star"
        assert _friendly_otype("SB*") == "Spectroscopic Binary Star"
        assert _friendly_otype("V*") == "Variable Star"

    def test_known_galaxy_subtypes(self):
        assert _friendly_otype("Sy2") == "Seyfert 2 Galaxy"
        assert _friendly_otype("AGN") == "Active Galactic Nucleus"
        assert _friendly_otype("GiP") == "Galaxy in Pair"
        assert _friendly_otype("LIN") == "LINER Galaxy"
        assert _friendly_otype("LINER") == "LINER Galaxy"
        assert _friendly_otype("EmG") == "Emission-Line Galaxy"
        assert _friendly_otype("SBG") == "Starburst Galaxy"

    def test_known_nebula_types(self):
        assert _friendly_otype("HII") == "Emission Nebula (HII Region)"
        assert _friendly_otype("PN") == "Planetary Nebula"
        assert _friendly_otype("SNR") == "Supernova Remnant"

    def test_known_cluster_types(self):
        assert _friendly_otype("GlC") == "Globular Cluster"
        assert _friendly_otype("OC") == "Open Cluster"

    def test_supergiant_variants(self):
        assert _friendly_otype("s*b") == "Blue Supergiant Star"
        assert _friendly_otype("s*r") == "Red Supergiant Star"
        assert _friendly_otype("s*y") == "Yellow Supergiant Star"
        assert _friendly_otype("sg*") == "Supergiant Star"

    def test_otype_labels_dict_checked_first(self):
        # otype_txt that echoes the raw code should NOT override the dict
        assert _friendly_otype("GiP", "GiP") == "Galaxy in Pair"
        assert _friendly_otype("Sy2", "Sy2") == "Seyfert 2 Galaxy"

    def test_otype_txt_fallback_only_when_useful(self):
        # An unknown otype with a real text description should use the text
        result = _friendly_otype("??X", "Exotic Object")
        assert result == "Exotic Object"

    def test_otype_txt_not_used_when_same_as_otype(self):
        # otype_txt that just echoes the raw code should NOT be used
        result = _friendly_otype("??X", "??x")
        assert result == "??X"  # falls back to raw otype

    def test_empty_otype(self):
        assert _friendly_otype("") == "Unknown"
        assert _friendly_otype("", "") == "Unknown"

    def test_whitespace_stripped(self):
        assert _friendly_otype("  Sy2  ") == "Seyfert 2 Galaxy"

    def test_all_dict_entries_are_non_empty_strings(self):
        for code, label in _OTYPE_LABELS.items():
            assert isinstance(label, str) and label.strip(), \
                f"_OTYPE_LABELS[{code!r}] is empty or not a string"


# ---------------------------------------------------------------------------
# _pick_common_name
# ---------------------------------------------------------------------------

class TestPickCommonName:

    # Hard-coded names
    def test_hardcoded_star(self):
        assert _pick_common_name("* alf CMa") == "Sirius"
        assert _pick_common_name("* bet Ori") == "Rigel"
        assert _pick_common_name("* alf Ori") == "Betelgeuse"

    def test_hardcoded_from_ids_field(self):
        # The hard-coded name should be found in the ids pipe-separated list
        assert _pick_common_name("NGC  1976", "NAME Orion Nebula|M  42|* alf Ori") == "Orion Nebula"

    def test_hardcoded_messier_from_common_names(self):
        assert _pick_common_name("M  42") == "Orion Nebula"
        assert _pick_common_name("M  1") == "Crab Nebula"
        assert _pick_common_name("M  31") == "Andromeda Galaxy"

    # Messier formatting
    def test_messier_number_formatting(self):
        # Messier without hard-coded entry → "M<number>" format
        assert _pick_common_name("M  81") == "M81"
        assert _pick_common_name("M  82") == "M82"
        assert _pick_common_name("M 106") == "M106"

    def test_messier_from_ids_beats_name_label(self):
        # M82 has NAME UMa A in ids — Messier should win
        ids = "M  82|NGC  3034|NAME UMa A|IRAS 09517+6954"
        assert _pick_common_name("M  82", ids) == "M82"

    def test_messier_beats_name_label_when_main_id_is_name(self):
        # If main_id is NAME X but ids contains M number, use Messier
        ids = "M  82|NGC  3034|NAME UMa A"
        result = _pick_common_name("NAME UMa A", ids)
        assert result == "M82"

    # NAME prefix – proper names
    def test_name_prefix_proper_name_returned(self):
        result = _pick_common_name("NAME Great Orion Nebula", "")
        assert result == "Great Orion Nebula"

    def test_name_prefix_proper_name_from_ids(self):
        ids = "M  42|NAME Great Orion Nebula|NGC  1976"
        # M42 is in _COMMON_NAMES → "Orion Nebula" wins over NAME
        result = _pick_common_name("NGC  1976", ids)
        assert result == "Orion Nebula"

    # NAME technical labels — must be SKIPPED
    def test_skip_name_label_uma_a(self):
        # "UMa A" matches the skip pattern → should not be used
        ids = "NGC  3034|NAME UMa A|IRAS 09517"
        result = _pick_common_name("NGC  3034", ids)
        assert result.startswith("NGC")

    def test_skip_name_label_roman_numeral(self):
        # "Cl NGC 1234 IV" style label should be skipped
        ids = "NGC  2244|NAME Cl NGC 2244 II"
        result = _pick_common_name("NGC  2244", ids)
        assert result.startswith("NGC")

    def test_skip_name_cluster_bracket(self):
        # Names starting with '[' (survey designations) should be skipped
        ids = "NAME Rosette Nebula|[KPS2012] B123"
        result = _pick_common_name("NGC  2237", ids)
        assert result == "Rosette Nebula"

    # NGC / IC fallback
    def test_ngc_from_ids(self):
        ids = "NGC  2841|PGC 26512"
        assert _pick_common_name("NGC  2841", ids).startswith("NGC")

    def test_ic_from_ids(self):
        ids = "IC  434|LBN 953"
        result = _pick_common_name("IC  434", ids)
        assert result.startswith("IC")

    # Bayer designation prettification
    def test_bayer_prettified(self):
        result = _pick_common_name("* alf Ori", "")
        # Should prefer hard-coded "Betelgeuse" but if not available → prettify
        assert result == "Betelgeuse"

    def test_bayer_prettified_unknown_star(self):
        result = _pick_common_name("* zet Pup", "")
        assert result == "Zeta Pup"

    def test_star_marker_stripped(self):
        result = _pick_common_name("* V* R Lyr", "")
        assert not result.startswith("* ")

    # Edge cases
    def test_empty_ids(self):
        result = _pick_common_name("NGC  1234", "")
        assert result.startswith("NGC")

    def test_empty_main_id(self):
        result = _pick_common_name("", "")
        assert result == ""


# ---------------------------------------------------------------------------
# _mag_description
# ---------------------------------------------------------------------------

class TestMagDescription:

    def test_very_bright_negative(self):
        desc = _mag_description(-1.46)  # Sirius
        assert "magnitude -1.5" in desc
        assert "brightest" in desc

    def test_very_bright_zero(self):
        desc = _mag_description(0.0)
        assert "brightest" in desc

    def test_bright(self):
        desc = _mag_description(1.0)
        assert "extremely bright" in desc

    def test_naked_eye(self):
        desc = _mag_description(2.5)
        assert "naked eye" in desc

    def test_naked_eye_dark_skies(self):
        desc = _mag_description(4.5)
        assert "dark skies" in desc

    def test_binoculars_easy(self):
        desc = _mag_description(6.5)
        assert "binoculars" in desc

    def test_binoculars(self):
        desc = _mag_description(8.4)
        assert "binoculars" in desc

    def test_telescope(self):
        desc = _mag_description(11.0)
        assert "telescope" in desc

    def test_bad_input_returns_empty(self):
        assert _mag_description(None) == ""
        assert _mag_description("bad") == ""
        assert _mag_description("") == ""

    def test_boundary_exactly_7(self):
        desc = _mag_description(7.0)
        assert "binoculars" in desc


# ---------------------------------------------------------------------------
# _safe_str
# ---------------------------------------------------------------------------

class TestSafeStr:

    def test_normal_string(self):
        assert _safe_str("Sirius") == "Sirius"

    def test_nan_returns_none(self):
        assert _safe_str("nan") is None
        assert _safe_str("NaN") is None

    def test_none_returns_none(self):
        assert _safe_str("none") is None
        assert _safe_str("None") is None

    def test_dash_returns_none(self):
        assert _safe_str("--") is None

    def test_empty_returns_none(self):
        assert _safe_str("") is None
        assert _safe_str("   ") is None

    def test_whitespace_stripped(self):
        assert _safe_str("  Sirius  ") == "Sirius"

    def test_numeric_string(self):
        assert _safe_str("6.9") == "6.9"

    def test_masked_array_returns_none(self):
        class FakeMasked:
            mask = True
            def __str__(self): return "masked"
        assert _safe_str(FakeMasked()) is None

    def test_unmasked_array(self):
        class FakeUnmasked:
            mask = False
            def __str__(self): return "Betelgeuse"
        assert _safe_str(FakeUnmasked()) == "Betelgeuse"


# ---------------------------------------------------------------------------
# _parse_natural_language
# ---------------------------------------------------------------------------

class TestParseNaturalLanguage:

    def test_brightest_stars_mode(self):
        mode, otype, con, lim = _parse_natural_language("10 brightest stars in the sky", 10)
        assert mode == "brightest_stars"
        assert otype is None
        assert con is None
        assert lim == 10

    def test_brightest_stars_in_constellation(self):
        mode, otype, con, lim = _parse_natural_language("10 brightest stars in Ursa Major", 10)
        assert mode == "constellation_objects"
        assert otype == "*"
        assert con == "UMA"
        assert lim == 10

    def test_brightest_stars_in_orion(self):
        mode, otype, con, _ = _parse_natural_language("brightest stars in Orion", 10)
        assert mode == "constellation_objects"
        assert otype == "*"
        assert con == "ORI"

    def test_extract_limit_from_query(self):
        _, _, _, lim = _parse_natural_language("list 5 brightest stars", 10)
        assert lim == 5

    def test_limit_capped_at_100(self):
        _, _, _, lim = _parse_natural_language("show me 999 galaxies", 10)
        assert lim == 100

    def test_constellation_galaxies(self):
        mode, otype, con, lim = _parse_natural_language("10 brightest galaxies in Ursa Major", 10)
        assert mode == "constellation_objects"
        assert otype == "G"
        assert con == "UMA"
        assert lim == 10

    def test_constellation_emission_nebulae(self):
        mode, otype, con, lim = _parse_natural_language("emission nebulae in Orion", 10)
        assert mode == "constellation_objects"
        assert otype == "HII"
        assert con == "ORI"

    def test_constellation_globular_clusters(self):
        mode, otype, con, lim = _parse_natural_language("globular clusters in Sagittarius", 10)
        assert mode == "constellation_objects"
        assert otype == "GlC"
        assert con == "SGR"

    def test_constellation_planetary_nebulae(self):
        mode, otype, con, lim = _parse_natural_language("planetary nebulae in Lyra", 10)
        assert mode == "constellation_objects"
        assert otype == "PN"
        assert con == "LYR"

    def test_general_mode_no_constellation(self):
        mode, otype, con, lim = _parse_natural_language("brightest quasars", 10)
        assert mode == "general"
        assert otype == "QSO"
        assert con is None

    def test_case_insensitive_constellation(self):
        mode, otype, con, _ = _parse_natural_language("galaxies in ursa major", 10)
        assert con == "UMA"

    def test_default_limit_used_when_no_number(self):
        _, _, _, lim = _parse_natural_language("emission nebulae in Orion", 7)
        assert lim == 7

    def test_open_clusters_in_taurus(self):
        mode, otype, con, _ = _parse_natural_language("open clusters in Taurus", 10)
        assert mode == "constellation_objects"
        assert otype == "OC"
        assert con == "TAU"


# ---------------------------------------------------------------------------
# _OTYPE_GROUPS
# ---------------------------------------------------------------------------

class TestOtypeGroups:

    def test_galaxy_group_includes_famous_subtypes(self):
        group = _OTYPE_GROUPS["G"]
        for expected in ("G", "Sy1", "Sy2", "AGN", "LINER", "EmG", "SBG"):
            assert expected in group, f"'{expected}' missing from G group"

    def test_galaxy_group_no_duplicates(self):
        group = _OTYPE_GROUPS["G"]
        assert len(group) == len(set(group))

    def test_all_group_members_have_otype_labels(self):
        # Every otype in a group should have a friendly label
        missing = []
        for group_key, members in _OTYPE_GROUPS.items():
            for m in members:
                if m not in _OTYPE_LABELS:
                    missing.append(f"{group_key} → {m}")
        assert not missing, f"Missing _OTYPE_LABELS entries: {missing}"


# ---------------------------------------------------------------------------
# _format_rows
# ---------------------------------------------------------------------------

class TestFormatRows:

    def _make_row(self, main_id, otype, vmag, ids="", ra="10.0", dec="20.0", otype_txt=""):
        """Return a dict that mimics an astropy table row."""
        return {
            "main_id": main_id,
            "otype": otype,
            "otype_txt": otype_txt,
            "vmag": vmag,
            "ids": ids,
            "ra": ra,
            "dec": dec,
        }

    def test_empty_rows(self):
        result = _format_rows([], "Test Title")
        assert "No objects found" in result

    def test_title_in_output(self):
        rows = [self._make_row("* alf CMa", "*", "-1.46", "* alf CMa")]
        result = _format_rows(rows, "Brightest Stars")
        assert "Brightest Stars" in result
        assert "=" * 15 in result

    def test_common_name_used(self):
        rows = [self._make_row("* alf CMa", "*", "-1.46", "* alf CMa")]
        result = _format_rows(rows, "Stars")
        assert "Sirius" in result

    def test_friendly_otype_used(self):
        rows = [self._make_row("M  81", "Sy2", "6.94")]
        result = _format_rows(rows, "Galaxies")
        assert "Seyfert 2 Galaxy" in result

    def test_magnitude_description_included(self):
        rows = [self._make_row("M  81", "Sy2", "6.94")]
        result = _format_rows(rows, "Galaxies")
        assert "magnitude 6.9" in result
        assert "binoculars" in result

    def test_position_included(self):
        rows = [self._make_row("M  81", "Sy2", "6.94", ra="148.9", dec="69.1")]
        result = _format_rows(rows, "Galaxies")
        assert "RA" in result
        assert "Dec" in result

    def test_source_footer(self):
        rows = [self._make_row("M  81", "Sy2", "6.94")]
        result = _format_rows(rows, "Galaxies")
        assert "SIMBAD" in result

    def test_numbered_list(self):
        rows = [
            self._make_row("M  81", "Sy2", "6.94"),
            self._make_row("M  82", "AGN", "8.41"),
        ]
        result = _format_rows(rows, "Galaxies")
        assert "1. " in result
        assert "2. " in result

    def test_northern_hemisphere_label(self):
        rows = [self._make_row("M  81", "Sy2", "6.94", ra="148.9", dec="69.1")]
        result = _format_rows(rows, "Galaxies")
        assert "northern sky" in result

    def test_southern_hemisphere_label(self):
        rows = [self._make_row("* alf Car", "*", "0.72", ra="95.9", dec="-52.7")]
        result = _format_rows(rows, "Stars")
        assert "southern sky" in result

    def test_no_magnitude_when_missing(self):
        rows = [self._make_row("SomeObj", "*", "nan")]
        result = _format_rows(rows, "Stars")
        assert "Brightness:" not in result

    def test_messier_name_formatting(self):
        rows = [self._make_row("M  82", "AGN", "8.41", ids="M  82|NGC  3034|NAME UMa A")]
        result = _format_rows(rows, "Galaxies")
        assert "M82" in result
        assert "UMa A" not in result
