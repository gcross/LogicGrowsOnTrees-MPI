-- Language extensions {{{
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

-- Imports {{{
import Control.Monad ((>=>))

import Data.Monoid (Sum(..),mempty)
import Data.Serialize (Serialize(..))

import Options.Applicative

import Control.Visitor.Main (TerminationReason(..),mainVisitor)
import Control.Visitor.Examples.Queens (nqueensCorrectCount,nqueensCount,nqueens_maximum_size)
import Control.Visitor.Parallel.MPI (driver)
-- }}}

instance Serialize (Sum Int) where
    put = put . getSum
    get = fmap Sum get

main =
    mainVisitor
        driver
        (argument (auto >=> \n → if n >= 1 && n <= nqueens_maximum_size then return n else fail $ "bad board size (must be between 1 and " ++ show nqueens_maximum_size ++ " inclusive)")
            (   metavar "#"
             <> help ("board size (must be between 1 and " ++ show nqueens_maximum_size ++ " inclusive)")
            )
        )
        mempty
        (\n termination_reason →
            case termination_reason of
                Aborted progress → error $ "Visitor aborted with progress " ++ show progress ++ "."
                Completed (Sum number_of_solutions)
                 | nqueensCorrectCount n == number_of_solutions →
                    putStrLn $ "Correctly found all " ++ show number_of_solutions ++ " solutions for board size " ++ show n ++ "."
                 | otherwise →
                    error $ "Found " ++ show number_of_solutions ++ " instead of " ++ show (nqueensCorrectCount n) ++ " solutions for board size " ++ show n ++ "."
                Failure description → error $ "Visitor failed with reason " ++ show description ++ "."
        )
        nqueensCount
