import os
import sys

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session

# get path to taskmanager. it is assumed that this script is in the bin directory of
# the taskmanager package.
tmpath = os.path.normpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) + '/..' ) )

libpath = '%s/lib' % tmpath
varpath = '%s/var' % tmpath

# include lib path of the TaskManager package to sys.path for loading TaskManager packages
sys.path.insert(0,libpath)

##from hDBSessionMaker import hDBSessionMaker
import hDBSessionRegistry

DBSession = hDBSessionRegistry.DBSession
engine = hDBSessionRegistry.engine


class hDBConnection( object ):
    def __init__( self, echo=False ):
        ### Create an engine that stores data in the local directory's
        ### sqlalchemy_example.db file.
        ###engine = create_engine( 'sqlite:///{varpath}/taskDispatcher.db'.format(varpath=varpath), echo=False )
        ##engine = create_engine( 'mysql://{user}:{password}@localhost/taskdispatcherdb'.format(user='tdadmin',password='tAskd1sPatcher'), echo=False )
 
        ### Create all tables in the engine. This is equivalent to "Create Table"
        ### statements in raw SQL.
        ##Base.metadata.create_all( engine )

        ###DBSession = sessionmaker( bind=engine )
        ### A DBSession() instance establishes all conversations with the database
        ### and represents a "staging zone" for all the objects loaded into the
        ### database session object. Any change made against the objects in the
        ### session won't be persisted into the database until you call
        ### session.commit(). If you're not happy about the changes, you can
        ### revert all of them back to the last commit by calling
        ### session.rollback()
        #self.dbSessionMaker = hDBSessionMaker( echo=echo )
        #self.session = self.dbSessionMaker.DBSession()
        self.session = DBSession()
        
        ##### @var ScopedSession
        ####The session that represents the connection to the database
        ###if not scopedSession:
        ###    self.ScopedSession = scoped_session( DBSession )
        ###else:
        ###    self.ScopedSession = scopedSession
        ###
        ##### @var session
        #### A ScopedSession() instance establishes all conversations with the database
        #### and represents a "staging zone" for all the objects loaded into the
        #### database session object. Any change made against the objects in the
        #### session won't be persisted into the database until you call
        #### session.commit(). If you're not happy about the changes, you can
        #### revert all of them back to the last commit by calling
        #### session.rollback()
        ###self.session = self.ScopedSession()
        
    #def __del__( self ):
    #    """! @brief Tidy up session upon destruction of the Connect object"""
    #    self.session.remove()

    def query( self, *args, **kwargs ):
        """! @brief Alias for session.query()
        
        @param args Arguments that should be forwarded to session.query
        @param kwargs Keyword arguments that should be forwarded to session.query
        
        @return A query object that can be further refined
        """
        
        return self.session.query( *args, **kwargs )

    def introduce( self, *objects ):
        """! Prepare objects for commit to the database
        
        @param objects Any number of database objects that should be introduced to the database
        @return self
        """
        for obj in objects:
            self.session.add( obj )
 
    def delete( self, *objects ):
        """! @brief Mark objects for deletion from the database.

        @param objects Any number of database objects that should be deleted.
        @return self
        """
        for obj in objects:
            self.session.delete( obj )
        return self

    def commit( self ):
        """! @brief Commit all objects that have been prepared
        
        @return return value from sqlalchemy's @c commit() method
        """
        return self.session.commit()

    def remove( self ):
        """! @brief tell registry to dispose of session
        """
        
        #self.dbSessionMaker.DBSession.remove()
        DBSession.remove()
        
    def create_all_tables( self ):
        """! brief This will not re-create tables that already exist
        """
        
        from hDatabase import Base
        #Base.metadata.create_all( self.dbSessionMaker.engine )
        Base.metadata.create_all( engine )

    def drop_all_tables( self ):
        """! brief This will really drop all tables including their contents."""

        #meta = MetaData( self.dbSessionMaker.engine )
        meta = MetaData( engine )
        meta.reflect()
        meta.drop_all()

