
from hDBConnection import hDBConnection
import hDatabase as db

class hJobSchedulerPrioritized( object ):
    def __init__( self ):
        pass

    def next( self, numJobs=1, excludedJobIDs=set([]), returnInstances=False ):
        """! @brief get next jobs which will be send to cluster

        @param numJobs (int) maximal number of jobs which will be returned
        @param excludedJobIDs (set) set of jobIDs which should not be considered
        @param returnInstances (bool) if True return db.Job instances otherwise return job ids

        @return (list) job ids or db.Job
        
        @todo think about something more sophisticated than just taking the next in queue
        """

        dbconnection = hDBConnection()

        #timeLogger.log( "number excluded jobs: {n}".format(n=len(excludedJobIDs)) )
        # get next waiting job in queue
        #timeLogger.log( "get jobs ..." )
        if excludedJobIDs:
            jobs = dbconnection.query( db.WaitingJob ).\
                   join( db.User ).\
                   join( db.Job ).\
                   filter( db.User.enabled==True ).\
                   filter( not_(db.Job.id.in_(excludedJobIDs) ) ).\
                   order_by( db.WaitingJob.priorityValue ).\
                   limit( numJobs ).all()
            
        else:
            jobs = dbconnection.query( db.WaitingJob ).\
                   join( db.User ).\
                   filter( db.User.enabled==True ).\
                   order_by( db.WaitingJob.priorityValue.desc() ).\
                   limit( numJobs ).all()
            
        #timeLogger.log( "number of found jobs: {n}".format(n=len(jobs)) )
        
        if jobs:
            # return job id
            if returnInstances:
                return jobs
            else:
                return [ j.job_id for j in jobs ]
        else:
            # no job was found
            return []

        
    def setPriorities( self ):
        """! @brief set priorities of all waiting jobs

        1.0 + priority.value)/128
        """
        pass
