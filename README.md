langohr-component-async
=======================

A version of the [langohr getting started example](http://clojurerabbitmq.info/articles/getting_started.html) that uses async channels and stuartsierra's component.

How to run it:

* make sure rabbitmq server is running and reachable at a location configured in `conf/q.clj`
* Start a repl with `lein repl`
* open test/user.clj in emacs
* emacs needs cider installed
* `C-c M-c` to connect emacs to repl
* in the repl, run `(start)` and then `(run-example)`


