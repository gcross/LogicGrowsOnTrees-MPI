Version 1.0.0.1
===============

* Improved performance.

* Now notices are logged when workers connect and disconnect.

* Exposed `getCurentStatistics` in the `RequestQueueMonad`, allowing one to
  obtain the statistics at any time during the run.

* Fixed the documentation.

* Now ImplicitParams are used instead of a monad to ensure sockets
  initialization.

* Fixed bug where MPI was not geting initialized.

* Bumped dependency bounds.

* Lowered he lower bound on `stm` from 2.4 to 2.3.

* Deleted vestigial import.
