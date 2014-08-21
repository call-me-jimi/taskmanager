import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, Float, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy import create_engine
 
Base = declarative_base()

## @brief association between user and role
#
class AssociationUserRole(Base):
    __tablename__ = 'association_user_role'
    
    user_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
    role_id = Column(Integer, ForeignKey('role.id'), primary_key=True)
    
    role = relationship("Role", backref="user_assocs")

## @brief user of taskmanager
#
class User( Base ):
    __tablename__ = 'user'

    id = Column( Integer, primary_key=True )
    name = Column( String(100) )
    tms_host = Column( String(100) )
    tms_port = Column( Integer(5) )
    tms_id = Column( String(100) )
    
    roles = relationship("AssociationUserRole", backref="user")
    
## @brief role of user
#
class Role( Base ):
    __tablename__ = 'role'

    id = Column( Integer, primary_key=True )
    name = Column( String(100) )

    
## @brief JobStatus
#
# status of a job, such as waiting, pending, running, finished
class JobStatus( Base ):
    __tablename__ = 'job_status'

    id = Column( Integer, primary_key=True )
    name = Column( String(32) )

    def __repr__( self ):
        return "JobStatus [{id}] {n}".format( id=self.id, n=self.name )

## @brief Job
#
class Job( Base ):
    __tablename__ = 'job'

    id = Column( Integer, primary_key=True )
    user_id = Column( Integer, ForeignKey('user.id') )
    
    command = Column( String(2048) )
    info_text = Column( String(512) )
    group = Column( String(256) )
    shell = Column( String(16) )
    stdout = Column( String(256) )
    stderr = Column( String(256) )
    logfile = Column( String(256) )
    #job_details_id = Column( Integer, ForeignKey( 'job_details.id' ) )
    excluded_hosts = Column( String(1024), default='[]' )
    slots = Column( Integer, default=1 )
    
    user = relationship( 'User' )
    job_details = relationship( 'JobDetails', uselist=False, backref='job' )
    
    def __repr__( self ):
        return "Job [{id}] command: {c}".format( id=self.id, c=self.command )

## @brief JobDetails
#
# backref: job --> Job
class JobDetails( Base ):
    __tablename__ = 'job_details'

    id = Column( Integer, primary_key=True )

    job_id = Column( Integer, ForeignKey('job.id') )
    job_status_id = Column( Integer, ForeignKey('job_status.id') )
    host_id = Column( Integer, ForeignKey('host.id') )
    pid = Column( Integer(3) )
    return_code = Column( Integer(2) )

    job_status = relationship( 'JobStatus', uselist=False )
    host = relationship( 'Host' )


    
##class JobDetails( Base ):
##    __tablename__ = 'job_details'
##
##    id = Column( Integer, primary_key=True )
##
##    host = Column( String(100) )
##    pid = Column( Integer )
##    return_code = Column( Integer(100) )
##    
##    
##        
#### @brief current job status
###
##class CurrentJobStatus( Base ):
##    __tablename__ = 'current_job_status'
##
##    id = Column( Integer, primary_key=True )
##    job_id = Column( Integer, ForeignKey('job.id') )
##    job_status_type_id = Column( Integer, ForeignKey('job_status_type.id') )
##
##    job = relationship( Job )
##    job_status_type = relationship( JobStatusType, uselist=False, backref="current_job_status" )

    
class JobHistory( Base ):
    __tablename__ = 'job_history'

    id = Column( Integer, primary_key=True )

    job_id = Column( Integer, ForeignKey( 'job.id' ) )
    datetime = Column( DateTime, default = datetime.datetime.now )
    job_status_id = Column( Integer, ForeignKey( 'job_status.id' ) )
    
    job = relationship( 'Job' )
    job_status = relationship( 'JobStatus' )
                     

##### @brief Computer cluster
####
#### backrefs: hosts -> list(Host)
###class Cluster( Base ):
###    __tablename__ = 'cluster'
###
###    id = Column( Integer, primary_key=True )
###
###    name = Column( String )
    
## @brief Host
#
# backrefs: host_summary -> HostSummary
#           host_load -> HostLoad
class Host( Base ):
    __tablename__ = 'host'

    id = Column( Integer, primary_key=True )

    full_name = Column( String(512) )
    short_name = Column( String(128) )
    max_number_occupied_slots = Column( Integer )
    number_slots = Column( Integer )
    additional_info = Column( String(512) )
    allow_info_server = Column( Boolean )
    info_server_port = Column( Integer )
    
    ##    cluster_id = Column( Integer, ForeignKey( 'cluster.id' ) )
    ##cluster = relationship( Cluster, backref="hosts" )

    
class HostSummary( Base ):
    __tablename__ = 'host_summary'

    id = Column( Integer, primary_key=True )

    host_id = Column( Integer, ForeignKey( 'host.id' ) )
    
    available = Column( Boolean, default=False )  # whether host is in principle available to be included
    reachable = Column( Boolean, default=False )  # whether host is reachable, i.e., ready to be included
    active = Column( Boolean, default=False )     # whether host is included in cluster
    number_occupied_slots = Column( Integer, default=0 )

    host = relationship( 'Host', uselist=False, backref="host_summary" )

    
class HostLoad( Base ):
    __tablename__ = 'host_load'

    id = Column( Integer, primary_key=True )
    
    datetime = Column( DateTime, default = datetime.datetime.now )
    
    loadavg_1min = Column( Float, default=0 )
    loadavg_5min = Column( Float, default=0 )
    loadavg_10min = Column( Float, default=0 )

    host_id = Column( Integer, ForeignKey( 'host.id' ) )

    host = relationship( 'Host', backref="host_load" )

    def __repr__( self ):
        return "Load of host {h}: {l1} {l2} {l3} at {d}".format( h=self.host_id, 
                                                                 l1=self.loadavg_1min, 
                                                                 l2=self.loadavg_5min, 
                                                                 l3=self.loadavg_10min,
                                                                 d=str(self.datetime) )
    
