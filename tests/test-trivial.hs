{-# LANGUAGE UnicodeSyntax #-}

import Control.Applicative ((<$>))

import Data.Monoid (mempty)

import System.Log.Logger (Priority(..),rootLoggerName,setLevel,updateGlobalLogger)

import Visitor.Parallel.Adapter.MPI
import Visitor.Parallel.Common.ExplorationMode
import Visitor.Parallel.Common.Worker (Purity(Pure))
import Visitor.Parallel.Main

main =
    -- updateGlobalLogger rootLoggerName (setLevel DEBUG) >>
    (runMPI $
        runExplorer
            (const AllMode)
            Pure
            (return ((),()))
            (const $ return ())
            (const $ return [()])
            (const . const $ return mempty)
            (const . const $ return ())
    ) >>= \x → case runTerminationReason . snd <$> x of
        Nothing → return ()
        Just (Aborted progress) → error $ "Explorer aborted with progress " ++ show progress ++ "."
        Just (Completed [()]) → putStrLn $ "Trivial search completed successfully."
        Just (Completed result) → error $ "Result was " ++ show result ++ " not [()]."
        Just (Failure description) → error $ "Explorer failed with reason " ++ show description ++ "."
