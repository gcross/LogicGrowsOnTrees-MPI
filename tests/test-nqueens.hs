{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

import Control.Monad ((>=>))

import Data.Functor ((<$>))
import Data.Monoid (Sum(..),mempty)
import Data.Serialize (Serialize(..))

import System.Console.CmdTheLine
import System.Log.Logger (Priority(..),rootLoggerName,setLevel,updateGlobalLogger)

import Visitor.Parallel.Main (TerminationReason(..),mainForVisitTree,runTerminationReason)
import Visitor.Examples.Queens (BoardSize(..),nqueensCorrectCount,nqueensCount,nqueens_maximum_size)
import Visitor.Parallel.BackEnd.MPI (driver)
import Visitor.Utils.WordSum (WordSum(..))

main =  do
    -- updateGlobalLogger rootLoggerName (setLevel DEBUG)
    mainForVisitTree
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
