{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

import Data.Functor ((<$>))

import System.Console.CmdTheLine
import System.Log.Logger (Priority(..),rootLoggerName,setLevel,updateGlobalLogger)

import LogicGrowsOnTrees.Examples.Queens (BoardSize(..),nqueensCorrectCount,nqueensCount)
import LogicGrowsOnTrees.Parallel.Adapter.MPI (driver)
import LogicGrowsOnTrees.Parallel.Main (TerminationReason(..),mainForExploreTree,runTerminationReason)
import LogicGrowsOnTrees.Utils.WordSum (WordSum(..))

main =  do
    -- updateGlobalLogger rootLoggerName (setLevel DEBUG)
    mainForExploreTree
        driver
        (getBoardSize <$> required (flip (pos 0) (posInfo
            {   posName = "BOARD_SIZE"
            ,   posDoc = "board size"
            }
        ) Nothing))
        (defTI { termDoc = "count the number of n-queens solutions for a given board size" })
        (\n run_outcome →
            case runTerminationReason run_outcome of
                Aborted progress → error $ "Explorer aborted with progress " ++ show progress ++ "."
                Completed (WordSum number_of_solutions)
                 | nqueensCorrectCount n == number_of_solutions →
                    putStrLn $ "Correctly found all " ++ show number_of_solutions ++ " solutions for board size " ++ show n ++ "."
                 | otherwise →
                    error $ "Found " ++ show number_of_solutions ++ " instead of " ++ show (nqueensCorrectCount n) ++ " solutions for board size " ++ show n ++ "."
                Failure progress description → error $ "Explorer failed with progress " ++ show progress ++ " and reason " ++ show description ++ "."
        )
        nqueensCount
