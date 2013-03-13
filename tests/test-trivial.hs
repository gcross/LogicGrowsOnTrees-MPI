-- Language extensions {{{
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

-- Imports {{{
import Control.Applicative ((<$>))
import Control.Visitor.Parallel.Main
import Control.Visitor.Parallel.BackEnd.MPI

import Data.Monoid (mempty)
-- }}}

main =
    (runMPI $
        runVisitor
            (return ())
            (const $ return ())
            (const $ return mempty)
            (const $ return ())
            (const $ return [()])
    ) >>= \x → case runTerminationReason . snd <$> x of
        Nothing → return ()
        Just (Aborted progress) → error $ "Visitor aborted with progress " ++ show progress ++ "."
        Just (Completed [()]) → putStrLn $ "Trivial search completed successfully."
        Just (Completed result) → error $ "Result was " ++ show result ++ " not [()]."
        Just (Failure description) → error $ "Visitor failed with reason " ++ show description ++ "."
