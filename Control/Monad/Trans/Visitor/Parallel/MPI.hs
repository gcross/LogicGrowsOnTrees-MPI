-- Language extensions {{{
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

module Control.Monad.Trans.Visitor.Parallel.MPI
    ( SupervisorRequests(..)
    , TerminationReason(..)
    , runMPI
    , runVisitor
    , runVisitorIO
    , runVisitorT
    ) where

-- Imports {{{
import Prelude hiding (catch)

import Control.Applicative (Applicative())
import Control.Arrow ((&&&),second)
import Control.Concurrent (forkIO,killThread,yield)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan
import Control.Exception (AsyncException(ThreadKilled),SomeException)
import Control.Monad (forever,forM_,join,liftM2,mapM_,unless,void,when)
import Control.Monad.CatchIO (MonadCatchIO(),catch,finally)
import Control.Monad.Fix (MonadFix())
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Class (lift)

import qualified Data.ByteString as BS
import Data.ByteString (packCStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.Derive.Serialize
import Data.DeriveTH
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

import Control.Monad.Trans.Visitor (Visitor,VisitorIO,VisitorT)
import Control.Monad.Trans.Visitor.Checkpoint
import Control.Monad.Trans.Visitor.Supervisor
import Control.Monad.Trans.Visitor.Worker
import Control.Monad.Trans.Visitor.Workload
-- }}}

-- Types {{{

data MessageForSupervisor result = -- {{{
    Failed String
  | Finished (VisitorProgress result)
  | ProgressUpdate (VisitorWorkerProgressUpdate result)
  | StolenWorkload (Maybe (VisitorWorkerStolenWorkload result))
  | WorkerQuit
  deriving (Eq,Show)
$(derive makeSerialize ''MessageForSupervisor)
-- }}}

data MessageForWorker result = -- {{{
    RequestProgressUpdate
  | RequestWorkloadSteal
  | Workload VisitorWorkload
  | QuitWorker
  deriving (Eq,Show)
$(derive makeSerialize ''MessageForWorker)
-- }}}

data SupervisorRequests result = -- {{{
    SupervisorRequests
    {   requestCurrentProgress :: IO (VisitorProgress result)
    ,   requestCurrentProgressAsync :: (VisitorProgress result → IO ()) → IO ()
    ,   requestGlobalProgressUpdate :: IO (VisitorProgress result)
    ,   requestGlobalProgressUpdateAsync :: (VisitorProgress result → IO ()) → IO ()
    }
-- }}}

data TerminationReason result = -- {{{
    Aborted (VisitorProgress result)
  | Completed result
  | Failure String
  deriving (Eq,Show)
-- }}}

newtype MPI α = MPI { unwrapMPI :: IO α } deriving (Applicative,Functor,Monad,MonadCatchIO,MonadFix,MonadIO)

-- }}}

-- Exposed Functions {{{

runMPI :: MPI α → IO α -- {{{
runMPI action = unwrapMPI $ ((initializeMPI >> action) `finally` finalizeMPI)
-- }}}

runVisitorIO :: -- {{{
    (Monoid result, Serialize result) ⇒
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorRequests result → IO ()) →
    VisitorIO result →
    MPI (Maybe (TerminationReason result))
runVisitorIO getStartingProgress runManagerLoop =
    genericRunVisitor getStartingProgress runManagerLoop
    .
    flip forkVisitorIOWorkerThread
-- }}}

runVisitorT :: -- {{{
    (Monoid result, Serialize result, Functor m, MonadIO m) ⇒
    (∀ α. m α → IO α) →
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorRequests result → IO ()) →
    VisitorT m result →
    MPI (Maybe (TerminationReason result))
runVisitorT runMonad getStartingProgress runManagerLoop =
    genericRunVisitor getStartingProgress runManagerLoop
    .
    flip (forkVisitorTWorkerThread runMonad)
-- }}}

runVisitor :: -- {{{
    (Monoid result, Serialize result) ⇒
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorRequests result → IO ()) →
    Visitor result →
    MPI (Maybe (TerminationReason result))
runVisitor getStartingProgress runManagerLoop =
    genericRunVisitor getStartingProgress runManagerLoop
    .
    flip forkVisitorWorkerThread
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
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_initializeMPI" c_initializeMPI :: IO CInt
initializeMPI =
    liftIO c_initializeMPI
    >>=
    flip unless (
        error "This MPI implementation does not have funneled thread support (i.e., MPI_THREAD_FUNNELED)."
    ) . toBool
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
    (Monoid result, Serialize result) ⇒
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorRequests result → IO ()) →
    ((VisitorWorkerTerminationReason result → IO ()) → VisitorWorkload → IO (VisitorWorkerEnvironment result)) →
    MPI (Maybe (TerminationReason result))
genericRunVisitor getStartingProgress runManagerLoop spawnWorker =
    getMPIInformation >>=
    \(i_am_supervisor,number_of_workers) →
        if i_am_supervisor
            then Just <$> runSupervisor number_of_workers getStartingProgress runManagerLoop
            else runWorker spawnWorker >> return Nothing
-- }}}

runSupervisor :: -- {{{
    ∀ result.
    (Monoid result, Serialize result) ⇒
    CInt →
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorRequests result → IO ()) →
    MPI (TerminationReason result)
runSupervisor number_of_workers getStartingProgress runManagerLoop = do
    requests ← liftIO $ newTChanIO
    progress_receivers ← liftIO $ newIORef []
    let supervisor_requests = SupervisorRequests
            {   requestCurrentProgress = requestCurrentProgress_ 
            ,   requestCurrentProgressAsync = requestCurrentProgressAsync_ 
            ,   requestGlobalProgressUpdate = requestGlobalProgressUpdate_
            ,   requestGlobalProgressUpdateAsync = requestGlobalProgressUpdateAsync_
            }
          where
            requestCurrentProgress_ = do
                current_progress ← IVar.new
                requestCurrentProgressAsync_ (IVar.write current_progress)
                IVar.blocking . IVar.read $ current_progress
            requestCurrentProgressAsync_ receiveProgress =
                atomically . writeTChan requests $
                    getCurrentProgress >>= liftIO . receiveProgress
            requestGlobalProgressUpdate_ = do
                current_progress ← IVar.new
                requestGlobalProgressUpdateAsync_ (IVar.write current_progress)
                IVar.blocking . IVar.read $ current_progress
            requestGlobalProgressUpdateAsync_ receiveProgress = do
                atomicModifyIORef progress_receivers ((receiveProgress:) &&& const ())
                atomically . writeTChan requests $ performGlobalProgressUpdate
    _ ← liftIO $ forkIO (runManagerLoop supervisor_requests)
    maybe_starting_progress ← liftIO getStartingProgress
    let supervisor_actions = VisitorSupervisorActions
            {   broadcast_progress_update_to_workers_action =
                    mapM_ (sendMessage RequestProgressUpdate)
            ,   broadcast_workload_steal_to_workers_action =
                    mapM_ (sendMessage RequestWorkloadSteal)
            ,   receive_current_progress_action = \progress → do
                    liftIO $ atomicModifyIORef progress_receivers (const [] &&& id) >>= mapM_ ($ progress)
            ,   send_workload_to_worker_action =
                    sendMessage . Workload
            }
    termination_reason ←
         runVisitorSupervisorMaybeStartingFrom
            maybe_starting_progress
            supervisor_actions
            (do mapM_ addWorker [1..number_of_workers]
                let processAllRequests =
                        liftIO (atomically $ tryReadTChan requests) >>=
                        maybe processAllMessages (\request → request >> processAllRequests)
                    processAllMessages =
                        lift tryReceiveMessage >>=
                        maybe (liftIO yield >> processAllRequests) (\(worker_id,message) → do
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
                                    error $ "Worker " ++ show worker_id ++ " quit prematurely."
                            processAllMessages
                       )
                processAllRequests
            )
        >>= \(VisitorSupervisorResult termination_reason _) → return $ case termination_reason of
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
    return termination_reason
-- }}}

runWorker :: -- {{{
    ∀ result.
    (Monoid result, Serialize result) ⇒
    ((VisitorWorkerTerminationReason result → IO ()) → VisitorWorkload → IO (VisitorWorkerEnvironment result)) →
    MPI ()
runWorker spawnWorker = do
    outgoing_messages ← liftIO newTChanIO
    worker_environment ← liftIO newEmptyMVar
    let enqueueMessage = atomically . writeTChan outgoing_messages
        processIncomingMessages =
            tryReceiveMessage >>=
            maybe processOutgoingMessages (\(_,message) →
                case message of
                    RequestProgressUpdate → do
                        processRequest sendProgressUpdateRequest ProgressUpdate
                        processIncomingMessages
                    RequestWorkloadSteal → do
                        processRequest sendWorkloadStealRequest StolenWorkload
                        processIncomingMessages
                    Workload workload → do
                        worker_is_running ← not <$> liftIO (isEmptyMVar worker_environment)
                        if worker_is_running
                            then failWorkerAlreadyRunning
                            else liftIO $
                                spawnWorker
                                    (\termination_reason → do
                                        _ ← takeMVar worker_environment
                                        case termination_reason of
                                            VisitorWorkerFinished final_progress →
                                                enqueueMessage (Finished final_progress :: MessageForSupervisor result)
                                            VisitorWorkerFailed exception →
                                                enqueueMessage (Failed (show exception))
                                            VisitorWorkerAborted →
                                                return ()
                                    )
                                    workload
                                 >>=
                                 putMVar worker_environment
                        processIncomingMessages
                    QuitWorker → do
                        sendMessage (WorkerQuit :: MessageForSupervisor result) 0
                        liftIO $
                            tryTakeMVar worker_environment
                            >>=
                            maybe (return ()) (killThread . workerThreadId)
            )
          where
            failure = flip sendMessage 0 . (Failed :: String → MessageForSupervisor result)
            failNoWorkerRunning = failure $
                "received a worker request when the worker was not running"
            failWorkerAlreadyRunning = failure $
                "received a workload then the worker was already running"
            processRequest sendRequest constructResponse =
                liftIO (tryTakeMVar worker_environment)
                >>=
                maybe failNoWorkerRunning (
                    \env@VisitorWorkerEnvironment{workerPendingRequests} → liftIO $ do
                        sendRequest workerPendingRequests $ enqueueMessage . constructResponse
                        putMVar worker_environment env
                )
        processOutgoingMessages =
            liftIO (atomically $ tryReadTChan outgoing_messages) >>=
            maybe
                (liftIO yield >> processIncomingMessages)
                (\message → sendMessage message 0 >> processOutgoingMessages)
    processIncomingMessages
-- }}}

-- }}}