TASKMANAGERPATH=$(shell pwd)
VE_PATH=$(TASKMANAGERPATH)/taskmanagerVE

all: ve rc

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
	cd $(VE_PATH) && \
	mkdir -p src && \
	cd src && \
	wget http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-5.6.22.tar.gz && \
	tar xzvf mysql-5.6.22.tar.gz && \
	cd mysql-5.6.22 && \
	cmake -DCMAKE_INSTALL_PREFIX=$(VE_PATH)/usr/mysql-5.6.22-taskmanagerdb && \
	make -j 2 install && \
	sed "s:{mysqlpath}:$(VE_PATH)/usr/mysql-5.6.22-taskmanagerdb:" $(TASKMANAGERPATH)/taskmanagerdb.cnf > $(VE_PATH)/usr/mysql-5.6.22-taskmanagerdb/taskmanagerdb.cnf

.PHONY: all rc ve sql
