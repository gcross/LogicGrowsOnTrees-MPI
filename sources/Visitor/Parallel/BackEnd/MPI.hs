{-# LANGUAGE FlexibleContexts #-}
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

{-| This back-end implements parallelism by via MPI.  Process 0 is the
    supervisor and the other processes are the workers.  (This does mean that
    one process is used only for coordination, but it simplifies things and also
    means that worker requests and responses will be handled promptly.)

    WARNING: Do *NOT* use the threaded runtime with this back-end as it has been
             designed with the assumption that the run-time is single-threaded.
             This was done because the MPI implementation might not support
             having multiple operating system threads (even if only one of them
             calls MPI functions), and anyway multiple operating system threads
             provide no benefit over lightweight Haskell threads in this case
             because the MPI scheduler will assign an MPI process to each CPU
             core so multiple threads will not result in better performance but
             rather in multiple processes fighting over the same CPU core, as
             well as the additional overhead of the threaded runtime compared to
             the non-threaded runtime. Lightweight Haskell threads, however, are
             just fine.
 -}
module Visitor.Parallel.BackEnd.MPI
    (
    -- * Driver
      driver
    , driverMPI
    -- * MPI
    -- ** Monad and runner
    , MPI
    , runMPI
    -- ** Information and communication
    , getMPIInformation
    , receiveBroadcastMessage
    , sendBroadcastMessage
    , sendMessage
    , tryReceiveMessage
    -- * Controller
    , MPIControllerMonad
    , abort
    , fork
    , getCurrentProgress
    , getCurrentProgressAsync
    , getNumberOfWorkers
    , getNumberOfWorkersAsync
    , requestProgressUpdate
    , requestProgressUpdateAsync
    -- * Generic runners
    -- $runners
    , runSupervisor
    , runWorker
    , runVisitor
    ) where


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
import System.Log.Logger (Priority(DEBUG))
import System.Log.Logger.TH

import Visitor (TreeGeneratorT)
import Visitor.Checkpoint
import Visitor.Parallel.Main
import qualified Visitor.Parallel.Common.Process as Process
import Visitor.Parallel.Common.Message
import Visitor.Parallel.Common.Supervisor hiding (getCurrentProgress,getNumberOfWorkers,runSupervisor)
import Visitor.Parallel.Common.Supervisor.RequestQueue
import Visitor.Parallel.Common.VisitorMode
import Visitor.Parallel.Common.Worker hiding (ProgressUpdate,StolenWorkload,runVisitor,runVisitorIO,runVisitorT)
import Visitor.Workload

--------------------------------------------------------------------------------
----------------------------------- Loggers ------------------------------------
--------------------------------------------------------------------------------

deriveLoggers "Logger" [DEBUG]

--------------------------------------------------------------------------------
------------------------------------ Driver ------------------------------------
--------------------------------------------------------------------------------

{-| This is the driver for the MPI back-end.  Process 0 acts as the supervisor
    and the other processes act as workers.

    WARNING: Do *NOT* use the threaded runtime with this driver (or 'driverMPI'),
             see the warning in the documentation for this module for more
             details.
 -}
driver ::
    ∀ shared_configuration supervisor_configuration m n visitor_mode.
    ( Serialize shared_configuration
    , Serialize (ProgressFor visitor_mode)
    , Serialize (WorkerFinalProgressFor visitor_mode)
    ) ⇒ Driver IO shared_configuration supervisor_configuration m n visitor_mode
 -- Note:  The Monoid constraint should not have been necessary, but the type-checker complains without it.
driver =
    case (driverMPI :: Driver MPI shared_configuration supervisor_configuration m n visitor_mode) of
        Driver runDriver → Driver (runMPI . runDriver)
{-| This is the same as 'driver', but runs in the 'MPI' monad.  Use this driver
    if you want to do other things within 'MPI' (such as starting another
    parallel visit) after the run completes.
 -}
driverMPI ::
    ( Serialize shared_configuration
    , Serialize (ProgressFor visitor_mode)
    , Serialize (WorkerFinalProgressFor visitor_mode)
    ) ⇒ Driver MPI shared_configuration supervisor_configuration m n visitor_mode
 -- Note:  The Monoid constraint should not have been necessary, but the type-checker complains without it.
driverMPI = Driver $ \DriverParameters{..} →
    runVisitor
        constructVisitorMode
        purity
        (mainParser (liftA2 (,) shared_configuration_term supervisor_configuration_term) program_info)
        initializeGlobalState
        constructTreeGenerator
        getStartingProgress
        constructManager
    >>=
    maybe (return ()) (liftIO . (notifyTerminated <$> fst . fst <*> snd . fst <*> snd))

--------------------------------------------------------------------------------
------------------------------------- MPI -------------------------------------
--------------------------------------------------------------------------------

{-| This monad exists in order to ensure that the MPI system is initialized
    before it is used and finalized when we are done;  all MPI operations are
    run within it, which itself is run by using the 'runMPI' function.
 -}
newtype MPI α = MPI { unwrapMPI :: IO α } deriving (Applicative,Functor,Monad,MonadCatchIO,MonadFix,MonadIO)

{-| Initilizes MPI, runs the 'MPI' action, and then finalizes MPI. -}
runMPI :: MPI α → IO α
runMPI action = unwrapMPI $ ((initializeMPI >> action) `finally` finalizeMPI)

{-| Gets the total number of processes and whether this process is process 0. -}
getMPIInformation :: MPI (Bool,CInt)
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
        error "The number of total processes must be at least 2 so there is at least 1 worker."
    return (i_am_supervisor,number_of_workers)

{-| Receves a message broadcast from process 0 (which must not be this process). -}
receiveBroadcastMessage :: Serialize α ⇒ MPI α
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

{-| Sends a message broadcast from this process, which must be process 0. -}
sendBroadcastMessage :: Serialize α ⇒ α → MPI ()
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_sendBroadcastMessage" c_sendBroadcastMessage :: Ptr CChar → CInt → IO ()
sendBroadcastMessage message = liftIO $
    unsafeUseAsCStringLen (encode message) $ \(p_message,size) →
        c_sendBroadcastMessage p_message (fromIntegral size)

{-| Sends a message to another process. -}
sendMessage :: Serialize α ⇒ α → CInt → MPI ()
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_sendMessage" c_sendMessage :: Ptr CChar → CInt → CInt → IO ()
sendMessage message destination = liftIO $
    unsafeUseAsCStringLen (encode message) $ \(p_message,size) →
        c_sendMessage p_message (fromIntegral size) destination

{-| Receives a message (along with the sending process id) if one is waiting to
    be received;  this function will not block if there are no messages
    available.
 -}
tryReceiveMessage :: Serialize α ⇒ MPI (Maybe (CInt,α))
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

--------------------------------------------------------------------------------
--------------------------------- Internal MPI ---------------------------------
--------------------------------------------------------------------------------

finalizeMPI :: MPI ()
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_finalizeMPI" c_finalizeMPI :: IO ()
finalizeMPI = liftIO c_finalizeMPI

initializeMPI :: MPI ()
foreign import ccall unsafe "Visitor-MPI.h Visitor_MPI_initializeMPI" c_initializeMPI :: IO ()
initializeMPI = liftIO c_initializeMPI

--------------------------------------------------------------------------------
---------------------------------- Controller ----------------------------------
--------------------------------------------------------------------------------

{-| This is the monad in which the MPI controller will run. -}
newtype MPIControllerMonad visitor_mode α = C { unwrapC :: RequestQueueReader visitor_mode CInt MPI α} deriving (Applicative,Functor,Monad,MonadCatchIO,MonadIO,RequestQueueMonad)

instance HasVisitorMode (MPIControllerMonad visitor_mode) where
    type VisitorModeFor (MPIControllerMonad visitor_mode) = visitor_mode

--------------------------------------------------------------------------------
------------------------------- Generic runners --------------------------------
--------------------------------------------------------------------------------

{- $runners
In this section the full functionality of this module is exposed in case one
does not want the restrictions of the driver interface.  If you decide to go in
this direction, then you need to decide whether you want to manually handle
factors such as deciding whether a process is the supervisor or a worker and the
propagation of configuration information to the worker or whether you want this
to be done automatically;  if you want full control then call 'runSupervisor'
in the supervisor process --- which *must* be process 0! --- and call
'runWorker' in the worker processes, otherwise call 'runVisitor'.

WARNING: Do *NOT* use the threaded runtime with this back-end; see the
         warning in the documentation for this module for more details.
 -}

type MPIMonad visitor_mode = SupervisorMonad visitor_mode CInt MPI

{-| This runs the supervisor;  it must be called in process 0. -}
runSupervisor ::
    ∀ visitor_mode.
    ( Serialize (ProgressFor visitor_mode)
    , Serialize (WorkerFinalProgressFor visitor_mode)
    ) ⇒
    CInt {-^ the number of workers -} →
    VisitorMode visitor_mode {-^ the visitor mode -} →
    ProgressFor visitor_mode {-^ the initial progress of the run -} →
    MPIControllerMonad visitor_mode () {-^ the controller of the supervisor -} →
    MPI (RunOutcomeFor visitor_mode) {-^ the outcome of the run -}
runSupervisor
    number_of_workers
    visitor_mode
    starting_progress
    (C controller)
 = do
    debugM "Creating request queue and forking controller thread..."
    request_queue ← newRequestQueue
    _ ← liftIO . forkIO $ runReaderT controller request_queue
    let broadcastProgressUpdateToWorkers = mapM_ (sendMessage RequestProgressUpdate)

        broadcastWorkloadStealToWorkers = mapM_ (sendMessage RequestWorkloadSteal)

        receiveCurrentProgress = receiveProgress request_queue

        sendWorkloadToWorker = sendMessage . StartWorkload

        tryGetRequest :: MPI (Maybe (Either (MPIMonad visitor_mode ()) (CInt,MessageForSupervisorFor visitor_mode)))
        tryGetRequest = do
            maybe_message ← tryReceiveMessage
            case maybe_message of
                Just message → return . Just . Right $ message
                Nothing → do
                    maybe_request ← tryDequeueRequest request_queue
                    case maybe_request of
                        Just request → return . Just . Left $ request
                        Nothing → return Nothing
    debugM "Entering supervisor loop..."
    supervisor_outcome ←
        runSupervisorStartingFrom
            visitor_mode
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
    debugM "Exited supervisor loop;  shutting down workers..."
    mapM_ (sendMessage QuitWorker) [1..number_of_workers]
    let confirmShutdown remaining_workers
          | Set.null remaining_workers = return ()
          | otherwise =
            (tryReceiveMessage :: MPI (Maybe (CInt,MessageForSupervisorFor visitor_mode))) >>=
            maybe (confirmShutdown remaining_workers) (\(worker_id,message) →
                case message of
                    WorkerQuit → confirmShutdown (Set.delete worker_id remaining_workers)
                    _ → confirmShutdown remaining_workers
            )
    confirmShutdown $ Set.fromList [1..number_of_workers]
    return $ extractRunOutcomeFromSupervisorOutcome supervisor_outcome

{-| Runs a worker; it must be called in all processes other than process 0. -}
runWorker ::
    ( Serialize (ProgressFor visitor_mode)
    , Serialize (WorkerFinalProgressFor visitor_mode)
    ) ⇒
    VisitorMode visitor_mode {-^ the mode in to visit the tree -} →
    Purity m n {-^ the purity of the tree generator -} →
    TreeGeneratorT m (ResultFor visitor_mode) {-^ the tree generator -} →
    MPI ()
runWorker
    visitor_mode
    purity
    tree_generator
 = liftIO $ do
    debugM "Entering worker loop..."
    Process.runWorker
        visitor_mode
        purity
        tree_generator
        (fix $ \receiveMessage → unwrapMPI tryReceiveMessage >>= maybe (threadDelay 1 >> receiveMessage) (return . snd))
        (unwrapMPI . flip sendMessage 0)
    debugM "Exited worker loop."

{-| Visits the given tree using MPI to achieve parallelism.

    This function grants access to all of the functionality of this back-end,
    rather than having to go through the more restricted driver interface. The
    signature of this function is very complicated because it is meant to be
    used in all processes, supervisor and worker alike.
 -}
runVisitor ::
    ∀ shared_configuration supervisor_configuration visitor_mode m n.
    ( Serialize shared_configuration
    , Serialize (ProgressFor visitor_mode)
    , Serialize (WorkerFinalProgressFor visitor_mode)
    ) ⇒
    (shared_configuration → VisitorMode visitor_mode) {-^ construct the visitor mode given the shared configuration -} →
    Purity m n {-^ the purity of the tree generator -} →
    IO (shared_configuration,supervisor_configuration) {-^ get the shared and supervisor-specific configuration information (run only on the supervisor) -} →
    (shared_configuration → IO ()) {-^ initialize the global state of the process given the shared configuration (run on both supervisor and worker processes) -} →
    (shared_configuration → TreeGeneratorT m (ResultFor visitor_mode)) {-^ construct the tree generator from the shared configuration (run only on the worker) -} →
    (shared_configuration → supervisor_configuration → IO (ProgressFor visitor_mode)) {-^ get the starting progress given the full configuration information (run only on the supervisor) -} →
    (shared_configuration → supervisor_configuration → MPIControllerMonad visitor_mode ()) {-^ construct the controller for the supervisor (run only on the supervisor) -} →
    MPI (Maybe ((shared_configuration,supervisor_configuration),RunOutcomeFor visitor_mode))
        {-^ if this process is the supervisor, then returns the outcome of the
            run as well as the configuration information wrapped in 'Just';
            otherwise, if this process is a worker, it returns 'Nothing'
         -}
runVisitor
    constructVisitorMode
    purity
    getConfiguration
    initializeGlobalState
    constructTreeGenerator
    getStartingProgress
    constructManager
  = debugM "Fetching number of processes and whether this is the supervisor process..." >>
    getMPIInformation >>=
    \(i_am_supervisor,number_of_workers) →
        if i_am_supervisor
            then do
                debugM "I am the supervisor process."
                debugM "Getting configuration..."
                configuration@(shared_configuration,supervisor_configuration) ←
                    liftIO (getConfiguration `onException` unwrapMPI (sendBroadcastMessage (Nothing :: Maybe shared_configuration)))
                debugM "Broacasting shared configuration..."
                sendBroadcastMessage (Just shared_configuration)
                debugM "Initializing global state..."
                liftIO $ initializeGlobalState shared_configuration
                debugM "Reading starting progress..."
                starting_progress ← liftIO (getStartingProgress shared_configuration supervisor_configuration)
                debugM "Running supervisor..."
                Just . (configuration,) <$>
                    runSupervisor
                        number_of_workers
                        (constructVisitorMode shared_configuration)
                        starting_progress
                        (constructManager shared_configuration supervisor_configuration)
            else do
                debugM "I am the worker process."
                debugM "Getting shared configuration from broadcast..."
                maybe_shared_configuration ← receiveBroadcastMessage
                case maybe_shared_configuration of
                    Nothing → return Nothing
                    Just shared_configuration → do
                        debugM "Initializing global state..."
                        liftIO $ initializeGlobalState shared_configuration
                        debugM "Running worker..."
                        runWorker
                            (constructVisitorMode shared_configuration)
                            purity
                            (constructTreeGenerator shared_configuration)
                        return Nothing
