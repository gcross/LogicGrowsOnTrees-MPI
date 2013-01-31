-- Language extensions {{{
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

-- Imports {{{
import Data.Monoid (Sum(..))
import Data.Serialize (Serialize(..))
import System.Environment (getArgs)
import System.Log.Logger

import Control.Monad.Trans.Visitor.Examples.Queens
import Control.Monad.Trans.Visitor.Parallel.MPI
-- }}}

instance Serialize (Sum Int) where
    put = put . getSum
    get = fmap Sum get

main = do
    -- updateGlobalLogger rootLoggerName (setLevel DEBUG)
    runMPI $
        runVisitor
            (getArgs >>= \args → case map reads args of
                [[(n,"")]]
                  | n >= 1 && n <= nqueens_maximum_size → return n
                  | otherwise → error $ "board size must be between 1 and " ++ show nqueens_maximum_size ++ " inclusive"
                _ → error "test-nqueens must be called with a single integer argument specifying the board size"
            )
            (const $ return Nothing)
            (const $ return ())
             nqueensCount
    >>= \(n,termination_reason) →
    case termination_reason of
        Nothing → return ()
        Just (Aborted progress) → error $ "Visitor aborted with progress " ++ show progress ++ "."
        Just (Completed (Sum number_of_solutions))
         | nqueensCorrectCount n == number_of_solutions →
            putStrLn $ "Correctly found all " ++ show number_of_solutions ++ " solutions for board size " ++ show n ++ "."
         | otherwise →
            error $ "Found " ++ show number_of_solutions ++ " instead of " ++ show (nqueensCorrectCount n) ++ " solutions for board size " ++ show n ++ "."
        Just (Failure description) → error $ "Visitor failed with reason " ++ show description ++ "."
