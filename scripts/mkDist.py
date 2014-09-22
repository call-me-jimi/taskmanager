# creates frozen execuatables and dependencies inside the directory
#
# python mkDist.py [<TARGETDIR>]
#
# TARGETDIR specifies target directory for executables. if TARGETDIR is not given, "bin/dist" will be used

import sys
import subprocess
import os
from bbfreeze import Freezer

if len(sys.argv)==2:
    distdir = sys.argv[1]
else:
    distdir = "bin/dist"

## it is assumed that you are in the root direcotry of the TaskManager package
sys.path.insert(0,"lib")

## instantiates a Freezer object
f = Freezer(distdir = distdir, includes=( "daemon",
                                          "hCommand",
                                          "hDatabase",
                                          "hDBConnection",
                                          "hDBSessionMaker",
                                          "hServerProxy",
                                          "hSocket",
                                          "hTaskDispatcherInfo",
                                          "hTaskManagerMenialServerInfo",
                                          "hTaskManagerServerInfo",
                                          "hTMUtils",
                                          "TaskDispatcher",
                                          "TimeLogger",
                                          "TMMS",
                                          "TMS" ) )


## add scripts
f.addScript("bin/python/hRunJob.py")
f.addScript("bin/python/hConnect.py")
f.addScript("bin/python/hRunTMS.py")
f.addScript("bin/python/hRunTMMS.py")
f.addScript("bin/python/hRunTaskDispatcher.py")

## starts the freezing process
f()

sys.stdout.write("\nexecutables have been created in directory %s.\n" % distdir)
sys.stdout.write("done.\n")
