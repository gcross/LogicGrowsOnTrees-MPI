-- Language extensions {{{
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

module Control.Monad.Trans.Visitor.Parallel.MPI
    ( TerminationReason(..)
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
import Control.Exception (AsyncException(ThreadKilled),SomeException)
import Control.Monad (forever,forM_,join,liftM2,mapM_,unless,void,when)
import Control.Monad.CatchIO (MonadCatchIO(..),finally)
import Control.Monad.Fix (MonadFix())
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT,ask,runReaderT)

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

import qualified System.Log.Logger as Logger

import Control.Monad.Trans.Visitor (Visitor,VisitorIO,VisitorT)
import Control.Monad.Trans.Visitor.Checkpoint
import Control.Monad.Trans.Visitor.Supervisor
import Control.Monad.Trans.Visitor.Supervisor.RequestQueue
import qualified Control.Monad.Trans.Visitor.Supervisor.RequestQueue.Monad as RQM
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

data TerminationReason result = -- {{{
    Aborted (VisitorProgress result)
  | Completed result
  | Failure String
  deriving (Eq,Show)
-- }}}

newtype MPI α = MPI { unwrapMPI :: IO α } deriving (Applicative,Functor,Monad,MonadCatchIO,MonadFix,MonadIO)

type SupervisorMonad result = VisitorSupervisorMonad result CInt MPI

newtype SupervisorControllerMonad result α = C { unwrapC :: RequestQueueReader result CInt MPI α} deriving (Applicative,Functor,Monad,MonadCatchIO,MonadIO)

-- }}}

-- Instances {{{

instance Monoid result ⇒ RQM.RequestQueueMonad (SupervisorControllerMonad result) where -- {{{
    type RequestQueueMonadResult (SupervisorControllerMonad result) = result
    abort = C ask >>= abort
    getCurrentProgressAsync callback = C (ask >>= flip getCurrentProgressAsync callback)
    getNumberOfWorkersAsync callback = C (ask >>= flip getNumberOfWorkersAsync callback)
    requestProgressUpdateAsync callback = C (ask >>= flip requestProgressUpdateAsync callback)
-- }}}

-- }}}

-- Exposed Functions {{{

runMPI :: MPI α → IO α -- {{{
runMPI action = unwrapMPI $ ((initializeMPI >> action) `finally` finalizeMPI)
-- }}}

runVisitorIO :: -- {{{
    (Monoid result, Serialize result) ⇒
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorControllerMonad result ()) →
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
    (SupervisorControllerMonad result ()) →
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
    (SupervisorControllerMonad result ()) →
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
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_initializeMPI" c_initializeMPI :: IO ()
initializeMPI = liftIO c_initializeMPI
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

-- Logging Functions {{{
debugM :: MonadIO m ⇒ String → m ()
debugM = liftIO . Logger.debugM "Worker"

infoM :: MonadIO m ⇒ String → m ()
infoM = liftIO . Logger.infoM "Worker"
-- }}}

-- Internal Functions {{{

genericRunVisitor :: -- {{{
    (Monoid result, Serialize result) ⇒
    (IO (Maybe (VisitorProgress result))) →
    (SupervisorControllerMonad result ()) →
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
    (SupervisorControllerMonad result ()) →
    MPI (TerminationReason result)
runSupervisor number_of_workers getStartingProgress runManagerLoop = do
    request_queue ← newRequestQueue
    _ ← liftIO . forkIO $ runReaderT (unwrapC runManagerLoop) request_queue
    maybe_starting_progress ← liftIO getStartingProgress
    let supervisor_actions = VisitorSupervisorActions
            {   broadcast_progress_update_to_workers_action =
                    mapM_ (sendMessage RequestProgressUpdate)
            ,   broadcast_workload_steal_to_workers_action =
                    mapM_ (sendMessage RequestWorkloadSteal)
            ,   receive_current_progress_action = receiveProgress request_queue
            ,   send_workload_to_worker_action =
                    sendMessage . Workload
            }
    termination_reason ←
         runVisitorSupervisorMaybeStartingFrom
            maybe_starting_progress
            supervisor_actions
            (do mapM_ addWorker [1..number_of_workers]
                forever $ do
                    lift tryReceiveMessage >>=
                        maybe (liftIO yield) (\(worker_id,message) → do
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
                        )
                    processAllRequests request_queue
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
    worker_environment ← liftIO newEmptyMVar
    let processIncomingMessages =
            tryReceiveMessage >>=
            maybe (liftIO (threadDelay 1) >> processIncomingMessages) (\(_,message) →
                case message of
                    RequestProgressUpdate → do
                        processRequest sendProgressUpdateRequest ProgressUpdate
                        processIncomingMessages
                    RequestWorkloadSteal → do
                        processRequest sendWorkloadStealRequest StolenWorkload
                        processIncomingMessages
                    Workload workload → do
                        infoM "Received workload."
                        debugM $ "Workload is: " ++ show workload
                        worker_is_running ← not <$> liftIO (isEmptyMVar worker_environment)
                        if worker_is_running
                            then failWorkerAlreadyRunning
                            else liftIO $
                                spawnWorker
                                    (\termination_reason → do
                                        _ ← takeMVar worker_environment
                                        case termination_reason of
                                            VisitorWorkerFinished final_progress →
                                                unwrapMPI $ sendMessage (Finished final_progress :: MessageForSupervisor result) 0
                                            VisitorWorkerFailed exception →
                                                unwrapMPI $ sendMessage (Failed (show exception) :: MessageForSupervisor result) 0
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
            failWorkerAlreadyRunning = failure $
                "received a workload then the worker was already running"
            processRequest ::
                (VisitorWorkerRequestQueue result → (α → IO ()) → IO ()) →
                (α → MessageForSupervisor result) →
                MPI ()
            processRequest sendRequest constructResponse =
                liftIO (tryTakeMVar worker_environment)
                >>=
                maybe (return ()) (
                    \env@VisitorWorkerEnvironment{workerPendingRequests} → liftIO $ do
                        sendRequest workerPendingRequests $ unwrapMPI . flip sendMessage 0 . constructResponse
                        putMVar worker_environment env
                )
    processIncomingMessages
-- }}}

-- }}}
