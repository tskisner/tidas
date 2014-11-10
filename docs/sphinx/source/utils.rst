
.. _utils:

Utilities
==============


Data Selection
------------------

e.g. blocks like this (year_month_day/hour_minute)

20100125/1324
20110317/1415
20120726/1305
20120623/1307


Match format is


(grp=.*(schm=field.*,dict=prop.*),intr=.*)/2012.*(grp=.*(schm=field.*,dict=prop.*),intr=.*)/13.*(grp=.*(schm=field.*,dict=prop.*),intr=.*)

grp=.*(schm=field.*,dict=prop.*),intr=.*,blk=2012.*(grp=.*(schm=field.*,dict=prop.*),intr=.*,blk=13.*(grp=.*(schm=field.*,dict=prop.*),intr=.*,blk=.*))

[blk=2012.*,(blk=13.*(blk=.*))

[grp=.*[schm=field.*,dict=prop.*],intr=base.*]/2012.* [grp=.*[schm=field.*,dict=prop.*],intr=base.*]/13.* [grp=.*[schm=field.*,dict=prop.*],intr=base.*]/.*

note there is no sub-block match on the root block.



Dictionary
------------

.. doxygenclass:: tidas::dict
        :members:

