-- Language extensions {{{
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

module Control.Visitor.Parallel.MPI
    ( TerminationReason(..)
    , driver
    , driverMPI
    , runMPI
    , runVisitor
    , runVisitorIO
    , runVisitorT
    ) where

-- Imports {{{
import Prelude hiding (catch)

import Control.Applicative (Applicative())
import Control.Arrow ((&&&),second)
import Control.Concurrent (forkIO,killThread,threadDelay,yield)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan
import Control.Exception (AsyncException(ThreadKilled),SomeException,onException,throwIO)
import Control.Monad (forever,forM_,join,liftM2,mapM_,unless,void,when)
import Control.Monad.CatchIO (MonadCatchIO(..),finally)
import Control.Monad.Fix (MonadFix())
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT,ask,runReaderT)

import qualified Data.ByteString as BS
import Data.ByteString (packCStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.Composition ((.*******))
import Data.Derive.Serialize
import Data.DeriveTH
import Data.Function (fix)
import Data.Functor ((<$>))
import Data.IORef
import qualified Data.IVar as IVar
import Data.IVar (IVar)
import Data.Monoid (Monoid())
import Data.Serialize
import qualified Data.Set as Set
import Data.Set (Set)

import Foreign.C.Types (CChar,CInt(..))
import Foreign.Marshal.Alloc (alloca,free)
import Foreign.Marshal.Utils (toBool)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peek)

import Options.Applicative (InfoMod,execParser,info)

import qualified System.Log.Logger as Logger

import Control.Visitor (Visitor,VisitorIO,VisitorT)
import Control.Visitor.Checkpoint
import Control.Visitor.Main
import qualified Control.Visitor.Parallel.Process as Process
import Control.Visitor.Parallel.Process (MessageForSupervisor(..),MessageForWorker(..))
import Control.Visitor.Supervisor hiding (runSupervisor)
import Control.Visitor.Supervisor.RequestQueue
import Control.Visitor.Worker
import Control.Visitor.Workload
-- }}}

-- Types {{{

newtype MPI α = MPI { unwrapMPI :: IO α } deriving (Applicative,Functor,Monad,MonadCatchIO,MonadFix,MonadIO)

type MPIMonad result = SupervisorMonad result CInt MPI

newtype MPIControllerMonad result α = C { unwrapC :: RequestQueueReader result CInt MPI α} deriving (Applicative,Functor,Monad,MonadCatchIO,MonadIO)

-- }}}

-- Instances {{{

instance Monoid result ⇒ RequestQueueMonad (MPIControllerMonad result) where -- {{{
    type RequestQueueMonadResult (MPIControllerMonad result) = result
    abort = C abort
    fork = C . fork . unwrapC
    getCurrentProgressAsync = C . getCurrentProgressAsync
    getNumberOfWorkersAsync = C . getNumberOfWorkersAsync
    requestProgressUpdateAsync = C . requestProgressUpdateAsync
-- }}}

-- }}}

-- Drivers {{{

driver :: ∀ configuration visitor result. (Monoid result, Serialize configuration) ⇒ Driver IO configuration visitor result -- {{{
 -- Note:  The Monoid constraint should not have been necessary, but the type-checker complains without it.
driver = case (driverMPI :: Driver MPI configuration visitor result) of { Driver runDriver → Driver (runMPI .******* runDriver) }
-- }}}

driverMPI :: (Monoid result, Serialize configuration) ⇒ Driver MPI configuration visitor result -- {{{
 -- Note:  The Monoid constraint should not have been necessary, but the type-checker complains without it.
driverMPI = Driver $ \forkVisitorWorkerThread configuration_parser infomod initializeGlobalState getMaybeStartingProgress notifyTerminated constructVisitor constructManager →
    genericRunVisitor
        forkVisitorWorkerThread
        (execParser (info configuration_parser infomod))
        initializeGlobalState
        getMaybeStartingProgress
        constructManager
        constructVisitor
    >>=
    maybe (return ()) (liftIO . uncurry notifyTerminated)
-- }}}

-- }}}

-- Exposed Functions {{{

runMPI :: MPI α → IO α -- {{{
runMPI action = unwrapMPI $ ((initializeMPI >> action) `finally` finalizeMPI)
-- }}}

runVisitor :: -- {{{
    (Serialize configuration, Monoid result, Serialize result) ⇒
    IO configuration →
    (configuration → IO ()) →
    (configuration → IO (Maybe (VisitorProgress result))) →
    (configuration → MPIControllerMonad result ()) →
    (configuration → Visitor result) →
    MPI (Maybe (configuration,TerminationReason result))
runVisitor = genericRunVisitor forkVisitorWorkerThread
-- }}}

runVisitorIO :: -- {{{
    (Serialize configuration, Monoid result, Serialize result) ⇒
    IO configuration →
    (configuration → IO ()) →
    (configuration → IO (Maybe (VisitorProgress result))) →
    (configuration → MPIControllerMonad result ()) →
    (configuration → VisitorIO result) →
    MPI (Maybe (configuration,TerminationReason result))
runVisitorIO = genericRunVisitor forkVisitorIOWorkerThread
-- }}}

runVisitorT :: -- {{{
    (Serialize configuration, Monoid result, Serialize result, Functor m, MonadIO m) ⇒
    (∀ α. m α → IO α) →
    IO configuration →
    (configuration → IO ()) →
    (configuration → IO (Maybe (VisitorProgress result))) →
    (configuration → MPIControllerMonad result ()) →
    (configuration → VisitorT m result) →
    MPI (Maybe (configuration,TerminationReason result))
runVisitorT = genericRunVisitor . forkVisitorTWorkerThread
-- }}}

-- }}}

-- Foreign Functions {{{

finalizeMPI :: MPI () -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_finalizeMPI" c_finalizeMPI :: IO ()
finalizeMPI = liftIO c_finalizeMPI
-- }}}

getMPIInformation :: MPI (Bool,CInt) -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_getMPIInformation" c_getMPIInformation :: Ptr CInt → Ptr CInt → IO ()
getMPIInformation = do
    (i_am_supervisor,number_of_workers) ← liftIO $
        alloca $ \p_i_am_supervisor →
        alloca $ \p_number_of_workers → do
            c_getMPIInformation p_i_am_supervisor p_number_of_workers
            liftM2 (,)
                (toBool <$> peek p_i_am_supervisor)
                (peek p_number_of_workers)
    unless (number_of_workers > 0) $
        error "The number of total processors must be at least 2 so there is at least 1 worker."
    return (i_am_supervisor,number_of_workers)
-- }}}

initializeMPI :: MPI () -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_initializeMPI" c_initializeMPI :: IO ()
initializeMPI = liftIO c_initializeMPI
-- }}}

receiveBroadcastMessage :: Serialize α ⇒ MPI α -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_receiveBroadcastMessage" c_receiveBroadcastMessage :: Ptr (Ptr CChar) → Ptr CInt → IO ()
receiveBroadcastMessage = liftIO $
    alloca $ \p_p_message →
    alloca $ \p_size → do
        c_receiveBroadcastMessage p_p_message p_size
        p_message ← peek p_p_message
        size ← fromIntegral <$> peek p_size
        message ← packCStringLen (p_message,fromIntegral size)
        free p_message
        return . either error id . decode $ message
-- }}}

sendBroadcastMessage :: Serialize α ⇒ α → MPI () -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_sendBroadcastMessage" c_sendBroadcastMessage :: Ptr CChar → CInt → IO ()
sendBroadcastMessage message = liftIO $
    unsafeUseAsCStringLen (encode message) $ \(p_message,size) →
        c_sendBroadcastMessage p_message (fromIntegral size)
-- }}}

sendMessage :: Serialize α ⇒ α → CInt → MPI () -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_sendMessage" c_sendMessage :: Ptr CChar → CInt → CInt → IO ()
sendMessage message destination = liftIO $
    unsafeUseAsCStringLen (encode message) $ \(p_message,size) →
        c_sendMessage p_message (fromIntegral size) destination
-- }}}

tryReceiveMessage :: Serialize α ⇒ MPI (Maybe (CInt,α)) -- {{{
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_tryReceiveMessage" c_tryReceiveMessage :: Ptr CInt → Ptr (Ptr CChar) → Ptr CInt → IO ()
tryReceiveMessage = liftIO $
    alloca $ \p_source →
    alloca $ \p_p_message →
    alloca $ \p_size → do
        c_tryReceiveMessage p_source p_p_message p_size
        source ← peek p_source
        if source == -1
            then return Nothing
            else do
                p_message ← peek p_p_message
                size ← fromIntegral <$> peek p_size
                message ← packCStringLen (p_message,fromIntegral size)
                free p_message
                return $ Just (source,either error id . decode $ message)
-- }}}

-- }}}

-- Internal Functions {{{

genericRunVisitor :: -- {{{
    (Serialize configuration, Monoid result, Serialize result) ⇒
    (
        (VisitorWorkerTerminationReason result → IO ()) →
        visitor →
        VisitorWorkload →
        IO (VisitorWorkerEnvironment result)
    ) →
    IO configuration →
    (configuration → IO ()) →
    (configuration → IO (Maybe (VisitorProgress result))) →
    (configuration → MPIControllerMonad result ()) →
    (configuration → visitor) →
    MPI (Maybe (configuration,TerminationReason result))
genericRunVisitor forkWorkerThread getConfiguration initializeGlobalState getStartingProgress constructManager constructVisitor =
    getMPIInformation >>=
    \(i_am_supervisor,number_of_workers) →
        if i_am_supervisor
            then Just <$> runSupervisor number_of_workers getConfiguration initializeGlobalState getStartingProgress constructManager
            else runWorker initializeGlobalState constructVisitor forkWorkerThread >> return Nothing
-- }}}

runSupervisor :: -- {{{
    ∀ configuration result.
    (Serialize configuration, Monoid result, Serialize result) ⇒
    CInt →
    IO configuration →
    (configuration → IO ()) →
    (configuration → IO (Maybe (VisitorProgress result))) →
    (configuration → MPIControllerMonad result ()) →
    MPI (configuration,TerminationReason result)
runSupervisor number_of_workers getConfiguration initializeGlobalState getStartingProgress constructManager = do
    configuration :: configuration ← liftIO (getConfiguration `onException` unwrapMPI (sendBroadcastMessage (Nothing  :: Maybe configuration)))
    sendBroadcastMessage (Just configuration)
    liftIO $ initializeGlobalState configuration
    maybe_starting_progress ← liftIO (getStartingProgress configuration)
    request_queue ← newRequestQueue
    _ ← liftIO . forkIO $ runReaderT (unwrapC $ constructManager configuration) request_queue
    let supervisor_actions = SupervisorActions
            {   broadcast_progress_update_to_workers_action =
                    mapM_ (sendMessage RequestProgressUpdate)
            ,   broadcast_workload_steal_to_workers_action =
                    mapM_ (sendMessage RequestWorkloadSteal)
            ,   receive_current_progress_action = receiveProgress request_queue
            ,   send_workload_to_worker_action =
                    sendMessage . Workload
            }
    termination_reason ←
        runSupervisorMaybeStartingFrom
            maybe_starting_progress
            supervisor_actions
            (do mapM_ addWorker [1..number_of_workers]
                forever $ do
                    lift tryReceiveMessage >>= maybe (liftIO yield) (\(worker_id,message) →
                        case message of
                            Failed description →
                                receiveWorkerFailure worker_id description
                            Finished final_progress →
                                receiveWorkerFinished worker_id final_progress
                            ProgressUpdate progress_update →
                                receiveProgressUpdate worker_id progress_update
                            StolenWorkload maybe_stolen_workload →
                                receiveStolenWorkload worker_id maybe_stolen_workload
                            WorkerQuit →
                                error $ "Worker " ++ show worker_id ++ " has quit prematurely."
                     )
                    processAllRequests request_queue
            )
        >>= \(SupervisorResult termination_reason _) → return $ case termination_reason of
            SupervisorAborted progress → Aborted progress
            SupervisorCompleted result → Completed result
            SupervisorFailure worker_id message → Failure $
                "Process " ++ show worker_id ++ " failed with message: " ++ show message
    mapM_ (sendMessage QuitWorker) [1..number_of_workers]
    let confirmShutdown remaining_workers
          | Set.null remaining_workers = return ()
          | otherwise =
            (tryReceiveMessage :: MPI (Maybe (CInt,MessageForSupervisor result))) >>=
            maybe (confirmShutdown remaining_workers) (\(worker_id,message) →
                case message of
                    WorkerQuit → confirmShutdown (Set.delete worker_id remaining_workers)
                    _ → confirmShutdown remaining_workers
            )
    confirmShutdown $ Set.fromList [1..number_of_workers]
    return (configuration,termination_reason)
-- }}}

runWorker :: -- {{{
    (Serialize configuration, Monoid result, Serialize result) ⇒
    (configuration → IO ()) →
    (configuration → visitor) →
    (
        (VisitorWorkerTerminationReason result → IO ()) →
        visitor →
        VisitorWorkload →
        IO (VisitorWorkerEnvironment result)
    ) →
    MPI ()
runWorker initializeGlobalState constructVisitor forkWorkerThread = do
    receiveBroadcastMessage
    >>=
    maybe (return ())
    (\configuration → liftIO $ do
        initializeGlobalState configuration
        Process.runWorker
            (fix $ \receiveMessage → unwrapMPI tryReceiveMessage >>= maybe (threadDelay 1 >> receiveMessage) (return . snd))
            (unwrapMPI . flip sendMessage 0)
            (flip forkWorkerThread . constructVisitor $ configuration)
    )
-- }}}

-- }}}
