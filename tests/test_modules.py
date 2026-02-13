import unittest
from types import SimpleNamespace

from mas.models import FilterArgs, FuzzyJoinArgs, SelectArgs
from mas.profile import fraunhofer_lscm_focus
from mas.runner import enrich_final_result_with_links
from mas.utils import clean, name_similarity, norm_name


class TestUtils(unittest.TestCase):
    def test_clean(self):
        self.assertEqual(clean("  a   b "), "a b")

    def test_norm_name(self):
        self.assertEqual(norm_name("Fraunhofer GmbH"), "fraunhofer")

    def test_name_similarity(self):
        self.assertGreater(name_similarity("Fraunhofer", "Fraunhofer Institut"), 0.5)


class TestModels(unittest.TestCase):
    def test_select_args_valid(self):
        args = SelectArgs(table="projectfacts", limit=10)
        self.assertEqual(args.limit, 10)

    def test_select_args_invalid_limit(self):
        with self.assertRaises(Exception):
            SelectArgs(table="projectfacts", limit=999)

    def test_filter_args(self):
        f = FilterArgs(column="name", op="ilike", value="fraunhofer")
        self.assertEqual(f.op, "ilike")

    def test_fuzzy_join_args(self):
        j = FuzzyJoinArgs(
            left_table="evi_bilanz_publications",
            right_table="projectfacts",
            left_key="company_name",
            right_key="name",
        )
        self.assertGreaterEqual(j.similarity_threshold, 0.0)


class TestProfileAndRunner(unittest.TestCase):
    def test_profile_shape(self):
        data = fraunhofer_lscm_focus()
        self.assertIn("focus_areas", data)
        self.assertGreater(len(data["focus_areas"]), 0)

    def test_enrich_links(self):
        pred = SimpleNamespace(
            process_result="No links.",
            trajectory={"observation_0": {"detail_url": "https://www.evi.gv.at/foo"}},
        )
        out = enrich_final_result_with_links(pred)
        self.assertIn("Evidence links:", out.process_result)


if __name__ == "__main__":
    unittest.main()
