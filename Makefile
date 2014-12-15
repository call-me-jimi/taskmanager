TASKMANAGERPATH=$(shell pwd)
export TASKMANAGERPATH

all:
	# generate a source file
	# necessary enironmental variables are set within this source files
	echo 'export TASKMANAGERPATH=$(TASKMANAGERPATH)/taskmanager'  > .taskmanagerrc
	echo 'export TASKMANAGER_VE_PATH=$(TASKMANAGERPATH)/taskmanagerVE'  >> .taskmanagerrc
	echo 'source $(TASKMANAGERPATH)/taskmanagerVE/bin/activate' >> .taskmanagerrc
	echo 'export PATH=$(TASKMANAGERPATH)/taskmanager/bin:$$PATH' >> .taskmanagerrc

.PHONY: all
