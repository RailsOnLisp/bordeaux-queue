
(in-package :common-lisp)

(defpackage :bordeaux-queue
  (:use :bordeaux-threads :common-lisp)
  (:export
   #:queue
   #:queue-blocking-read
   #:queue-blocking-write
   #:queue-blocking
   #:enqueue
   #:dequeue
   #:dequeue-all))
