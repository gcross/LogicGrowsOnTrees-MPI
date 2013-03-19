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

module Control.Visitor.Parallel.BackEnd.MPI
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

import Control.Applicative ((<$>),(<*>),Applicative(),liftA2)
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
import Data.Composition ((.********))
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

import qualified System.Log.Logger as Logger

import Control.Visitor (Visitor,VisitorIO,VisitorT)
import Control.Visitor.Checkpoint
import Control.Visitor.Parallel.Main
import qualified Control.Visitor.Parallel.Common.Process as Process
import Control.Visitor.Parallel.Common.Message (MessageForSupervisor(..),MessageForWorker(..))
import Control.Visitor.Parallel.Common.Supervisor hiding (runSupervisor)
import Control.Visitor.Parallel.Common.Supervisor.RequestQueue
import Control.Visitor.Parallel.Common.Worker hiding (ProgressUpdate,StolenWorkload,runVisitor,runVisitorIO,runVisitorT)
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

driver :: -- {{{
    ∀ shared_configuration supervisor_configuration visitor result.
    ( Monoid result
    , Serialize shared_configuration
    ) ⇒ Driver IO shared_configuration supervisor_configuration visitor result
 -- Note:  The Monoid constraint should not have been necessary, but the type-checker complains without it.
driver = case (driverMPI :: Driver MPI shared_configuration supervisor_configuration visitor result) of { Driver runDriver → Driver (runMPI .******** runDriver) }
-- }}}

driverMPI :: -- {{{
    ( Monoid result
    , Serialize shared_configuration
    ) ⇒ Driver MPI shared_configuration supervisor_configuration visitor result
 -- Note:  The Monoid constraint should not have been necessary, but the type-checker complains without it.
driverMPI = Driver $
    \forkVisitorWorkerThread
     shared_configuration_term
     supervisor_configuration_term
     term_info
     initializeGlobalState
     constructVisitor
     getStartingProgress
     notifyTerminated
     constructManager →
    genericRunVisitor
        forkVisitorWorkerThread
        (mainParser (liftA2 (,) shared_configuration_term supervisor_configuration_term) term_info)
        initializeGlobalState
        constructVisitor
        getStartingProgress
        constructManager
    >>=
    maybe (return ()) (liftIO . (notifyTerminated <$> fst . fst <*> snd . fst <*> snd))
-- }}}

-- }}}

-- Exposed Functions {{{

runMPI :: MPI α → IO α -- {{{
runMPI action = unwrapMPI $ ((initializeMPI >> action) `finally` finalizeMPI)
-- }}}

runVisitor :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    IO (shared_configuration,supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → Visitor result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → MPIControllerMonad result ()) →
    MPI (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
runVisitor = genericRunVisitor forkVisitorWorkerThread
-- }}}

runVisitorIO :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    IO (shared_configuration,supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → VisitorIO result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → MPIControllerMonad result ()) →
    MPI (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
runVisitorIO = genericRunVisitor forkVisitorIOWorkerThread
-- }}}

runVisitorT :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result, Functor m, MonadIO m) ⇒
    (∀ α. m α → IO α) →
    IO (shared_configuration,supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → VisitorT m result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → MPIControllerMonad result ()) →
    MPI (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
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
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    (
        (WorkerTerminationReason result → IO ()) →
        visitor →
        Workload →
        IO (WorkerEnvironment result)
    ) →
    IO (shared_configuration,supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → visitor) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → MPIControllerMonad result ()) →
    MPI (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
genericRunVisitor forkWorkerThread getConfiguration initializeGlobalState constructVisitor getStartingProgress constructManager =
    getMPIInformation >>=
    \(i_am_supervisor,number_of_workers) →
        if i_am_supervisor
            then Just <$> runSupervisor number_of_workers getConfiguration initializeGlobalState getStartingProgress constructManager
            else runWorker initializeGlobalState constructVisitor forkWorkerThread >> return Nothing
-- }}}

runSupervisor :: -- {{{
    ∀ shared_configuration supervisor_configuration result.
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    CInt →
    IO (shared_configuration,supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → MPIControllerMonad result ()) →
    MPI ((shared_configuration,supervisor_configuration),RunOutcome result)
runSupervisor number_of_workers getConfiguration initializeGlobalState getStartingProgress constructManager = do
    configuration@(shared_configuration,supervisor_configuration) :: (shared_configuration,supervisor_configuration) ←
        liftIO (getConfiguration `onException` unwrapMPI (sendBroadcastMessage (Nothing :: Maybe shared_configuration)))
    sendBroadcastMessage (Just shared_configuration)
    liftIO $ initializeGlobalState shared_configuration
    starting_progress ← liftIO (getStartingProgress shared_configuration supervisor_configuration)
    request_queue ← newRequestQueue
    _ ← liftIO . forkIO $ runReaderT (unwrapC $ constructManager shared_configuration supervisor_configuration) request_queue
    let broadcastProgressUpdateToWorkers = mapM_ (sendMessage RequestProgressUpdate)
        broadcastWorkloadStealToWorkers = mapM_ (sendMessage RequestWorkloadSteal)
        receiveCurrentProgress = receiveProgress request_queue
        sendWorkloadToWorker = sendMessage . StartWorkload
        tryGetRequest :: MPI (Maybe (Either (MPIMonad result ()) (CInt,MessageForSupervisor result)))
        tryGetRequest = do -- {{{
            maybe_message ← tryReceiveMessage
            case maybe_message of
                Just message → return . Just . Right $ message
                Nothing → do
                    maybe_request ← tryDequeueRequest request_queue
                    case maybe_request of
                        Just request → return . Just . Left $ request
                        Nothing → return Nothing
        -- }}}
    SupervisorOutcome{..} ←
        runSupervisorStartingFrom
            starting_progress
            (SupervisorCallbacks{..})
            (PollingProgram
                (mapM_ addWorker [1..number_of_workers])
                tryGetRequest
                .
                either id
                $
                \(worker_id,message) → case message of
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
    let termination_reason = case supervisorTerminationReason of
            SupervisorAborted progress → Aborted progress
            SupervisorCompleted result → Completed result
            SupervisorFailure worker_id message → Failure $
                "Process " ++ show worker_id ++ " failed with message: " ++ show message
    return (configuration,RunOutcome supervisorRunStatistics termination_reason)
-- }}}

runWorker :: -- {{{
    (Serialize configuration, Monoid result, Serialize result) ⇒
    (configuration → IO ()) →
    (configuration → visitor) →
    (
        (WorkerTerminationReason result → IO ()) →
        visitor →
        Workload →
        IO (WorkerEnvironment result)
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
