import subprocess
from hop import version


def test_version():
    process = subprocess.run(
        ['hop', 'version'], stdout=subprocess.PIPE, universal_newlines=True)

    versions = process.stdout
    packages = version.get_packages()
    assert all(package in versions for package in packages)
