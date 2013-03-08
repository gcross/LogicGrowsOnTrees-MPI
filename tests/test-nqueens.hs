-- Language extensions {{{
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

-- Imports {{{
import Control.Monad ((>=>))

import Data.Functor ((<$>))
import Data.Monoid (Sum(..),mempty)
import Data.Serialize (Serialize(..))

import System.Console.CmdTheLine

import Control.Visitor.Main (TerminationReason(..),mainVisitor,runTerminationReason)
import Control.Visitor.Examples.Queens (BoardSize(..),nqueensCorrectCount,nqueensCount,nqueens_maximum_size)
import Control.Visitor.Parallel.MPI (driver)
import Control.Visitor.Utils.WordSum (WordSum(..))
-- }}}

main =
    mainVisitor
        driver
        (getBoardSize <$> required (flip (pos 0) (posInfo
            {   posName = "BOARD_SIZE"
            ,   posDoc = "board size"
            }
        ) Nothing))
        (defTI { termDoc = "count the number of n-queens solutions for a given board size" })
        (\n run_outcome →
            case runTerminationReason run_outcome of
                Aborted progress → error $ "Visitor aborted with progress " ++ show progress ++ "."
                Completed (WordSum number_of_solutions)
                 | nqueensCorrectCount n == number_of_solutions →
                    putStrLn $ "Correctly found all " ++ show number_of_solutions ++ " solutions for board size " ++ show n ++ "."
                 | otherwise →
                    error $ "Found " ++ show number_of_solutions ++ " instead of " ++ show (nqueensCorrectCount n) ++ " solutions for board size " ++ show n ++ "."
                Failure description → error $ "Visitor failed with reason " ++ show description ++ "."
        )
        nqueensCount
