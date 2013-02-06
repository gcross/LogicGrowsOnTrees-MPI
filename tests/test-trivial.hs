-- Language extensions {{{
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

-- Imports {{{
import Control.Visitor.Parallel.MPI
-- }}}

main =
    (runMPI $
        runVisitor
            (return ())
            (const $ return ())
            (const $ return Nothing)
            (const $ return ())
            (const $ return [()])
    ) >>= (\x → case x of
        Nothing → return ()
        Just ((),Aborted progress) → error $ "Visitor aborted with progress " ++ show progress ++ "."
        Just ((),Completed [()]) → putStrLn $ "Trivial search completed successfully."
        Just ((),Completed result) → error $ "Result was " ++ show result ++ " not [()]."
        Just ((),Failure description) → error $ "Visitor failed with reason " ++ show description ++ "."
    )