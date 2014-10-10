TaskManager
===========

TaskManager is an open source infrastructure software for distributing and managing calculation jobs
in a Unix computer cluster environment. The TaskManager was designed to control a set of hosts even
if you are not the administrator of the system. The hosts are embedded in a Unix environment and the
user's home directories are mounted on each host. The hosts may have different numbers of CPUs/cores
and operating systems. Keep in mind that a user should be able to log into each host via
ssh. However, users should use the TaskManager to submit calculation jobs to the cluster to avoid an
overload of the hosts and to a a proper scheduling. Jobs which are under the control of the
TaskManager are executed on a host of the computer cluster with the rights of the respective user to
ensure that the executing jobs have the permission to access the user's files.

The TaskManager package consists of several servers, TaskDispatcher, TaskManagerServer, and
TaskManagerMenialServer, which communicate with each other. Furthermore, clients may send requests
to one of these servers. Behind all servers there is a MySQL database. The main server,
TaskDispatcher, stores detailed information about jobs of users and sends jobs to vacant hosts of
the cluster, if possible. A TaskManagerServer (TMS) is automatically invoked by each user,
respectively. The user sends all his requests (usually via a client) to his own TMS, which in turn
communicates with the TaskDispatcher. A TaskManagerMenialServer (TMMS) is invoked automatically by
each user on each host. The TMMS is responsible for executing the user's jobs. The TaskDispatcher
and TMMS store and modify all information about jobs, hosts, users in the database. Other
clients/applications could access this database.


Please consult the file README for further details.