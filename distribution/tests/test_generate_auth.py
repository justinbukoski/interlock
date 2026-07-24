import importlib.util
import json
import pathlib
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).parents[1] / "generate-auth.py"
SPEC = importlib.util.spec_from_file_location("generate_auth", MODULE_PATH)
assert SPEC and SPEC.loader
generate_auth = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generate_auth)


class GenerateAuthTests(unittest.TestCase):
    def test_rerun_preserves_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory)
            auth = state / "auth.json"
            identity_path = state / "identity.json"

            identity = generate_auth.load_or_create_identity(identity_path, auth)
            first = dict(identity)
            second = generate_auth.load_or_create_identity(identity_path, auth)

            self.assertEqual(first, second)
            self.assertEqual(json.loads(identity_path.read_text()), first)

    def test_existing_auth_is_migrated_without_identity_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory)
            auth = state / "auth.json"
            expected = {
                "tenant_id": "11111111-1111-4111-8111-111111111111",
                "user_id": "22222222-2222-4222-8222-222222222222",
                "consumer_id": "33333333-3333-4333-8333-333333333333",
            }
            auth.write_text(json.dumps({"tokens": [{**expected, "role": "reader"}]}))

            identity = generate_auth.load_or_create_identity(
                state / "identity.json", auth
            )

            self.assertEqual(identity, expected)

    def test_identity_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory)
            target = state / "outside.json"
            target.write_text("{}")
            (state / "identity.json").symlink_to(target)

            with self.assertRaisesRegex(ValueError, "regular file"):
                generate_auth.load_or_create_identity(
                    state / "identity.json", state / "auth.json"
                )


if __name__ == "__main__":
    unittest.main()
