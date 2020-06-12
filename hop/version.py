import pkg_resources
import confluent_kafka

__author__ = "Shereen Elsayed (s_elsayed@ucsb.edu"
__description__ = "Print the versions of the packages that hop-client depends on"


def print_packages_versions():
    """ Print versions for the passed packages

    """
    packages = get_packages()
    for pkg in packages:
        if pkg == "librdkafka":
            print("%s==%s" % (pkg, confluent_kafka.libversion()[0]))
        else:
            print("%s==%s" % (pkg, pkg_resources.get_distribution(pkg).version))
    

def get_packages():
    return ["hop-client", "adc_streaming", "confluent_kafka", "librdkafka"]


# ------------------------------------------------
# -- main


def _main(args=None):
    """List all the dependencies' versions

    """

    print_packages_versions()
    