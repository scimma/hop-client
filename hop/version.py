import pkg_resources
import confluent_kafka


def print_packages_versions():
    """Print versions for the passed packages.

    """
    packages = get_packages()
    for pkg in packages:
        if pkg == "librdkafka":
            print("%s==%s" % (pkg, confluent_kafka.libversion()[0]))
        else:
            print("%s==%s" % (pkg, pkg_resources.get_distribution(pkg).version))


def get_packages():
    """Returns the package dependencies used within hop-client.

    """
    return ["hop-client", "adc_streaming", "confluent_kafka", "librdkafka"]


def _main(args):
    """List all the dependencies' versions.

    """

    print_packages_versions()
