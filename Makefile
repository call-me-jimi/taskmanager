TASKMANAGERPATH=$(shell pwd)
VE_PATH=$(TASKMANAGERPATH)/taskmanagerVE

all: ve rc sql

ve:
	virtualenv taskmanagerVE

rc:
	# generate a source file
	# necessary enironmental variables are set within this source files
	echo 'export TASKMANAGERPATH=$(TASKMANAGERPATH)/taskmanager'  > .taskmanagerrc
	echo 'export TASKMANAGER_VE_PATH=$(TASKMANAGERPATH)/taskmanagerVE'  >> .taskmanagerrc
	echo ''  >> .taskmanagerrc
	echo 'source $(TASKMANAGERPATH)/taskmanagerVE/bin/activate' >> .taskmanagerrc
	echo ''  >> .taskmanagerrc
	echo 'export PATH=$(TASKMANAGERPATH)/taskmanager/bin:$$PATH' >> .taskmanagerrc

sql:
	# install mysql (required by python package MySQLdb)
	cd $(VE_PATH); \
	mkdir -p src; \
	cd src; \
	wget http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-5.6.22.tar.gz; \
	tar xzvf mysql-5.6.22.tar.gz; \
	cd mysql-5.6.22; \
	cmake .; \
	make -j 6 install DESTDIR=$(VE_PATH)/usr/mysql-5.6.22; \
	cd $(VE_PATH)/usr/mysql-5.6.22; \
	mv usr/local/mysql/* . ;\
	rm -rf usr; \
	ln -s $(VE_PATH)/usr/mysql-5.6.22/bin/* $(VE_PATH)/bin/

.PHONY: all
