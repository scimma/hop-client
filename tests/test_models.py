#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module that tests models"


from hop import models


def test_voevent(voevent_fileobj):
    voevent = models.VOEvent.from_xml(voevent_fileobj)

    # check a few attributes
    assert voevent.ivorn == "ivo://gwnet/LVC#S200302c-1-Preliminary"
    assert voevent.role == "observation"
    assert voevent.version == "2.0"

    assert voevent.Who["Date"] == "2020-03-02T02:00:09"
    assert voevent.Description == "Report of a candidate gravitational wave event"
    assert (
        voevent.WhereWhen["ObsDataLocation"]["ObservatoryLocation"]["id"]
        == "LIGO Virgo"
    )
