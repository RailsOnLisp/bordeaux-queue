
(in-package :bordeaux-queue)

(deftype fixnum+ (&optional (low 0))
  `(integer ,low ,most-positive-fixnum))

(deftype fixnum* (&optional (low 1))
  `(integer ,low ,most-positive-fixnum))

(defclass queue ()
  ((vector :initarg :vector
	   :reader queue-vector
	   :type simple-vector)
   (read-index :initform 0
	       :accessor queue-read-index
	       :type fixnum+)
   (write-index :initform 0
		:accessor queue-write-index
		:type fixnum+)
   (length :initform 0
	   :accessor queue-length
	   :type fixnum+)
   (lock :initform (make-lock "queue")
	 :reader queue-lock)))

(defclass queue-blocking-read (queue)
  ((blocking-read-cv :initform (make-condition-variable
				:name "queue blocking read")
		      :reader queue-blocking-read-cv)))

(defclass queue-blocking-write (queue)
  ((blocking-write-cv :initform (make-condition-variable
				 :name "queue blocking write")
		      :reader queue-blocking-write-cv)))

(defclass queue-blocking (queue-blocking-read queue-blocking-write)
  ())

(defgeneric queue-full (queue))
(defgeneric on-enqueue (queue))
(defgeneric enqueue (queue item &optional blocking))

(defgeneric queue-empty (queue))
(defgeneric queue-peek (queue &optional blocking))

(defgeneric on-dequeue (queue))
(defgeneric dequeue (queue &optional blocking))
(defgeneric dequeue-all (queue))

;;  Initialization

(defun make-queue-vector (size &optional (element-type 't) initial-element)
  (declare (type fixnum* size))
  (assert (typep initial-element element-type))
  (make-array `(,size)
	      :element-type element-type
	      :initial-element initial-element))

(defmethod initialize-instance ((q queue) &rest initargs
				&key size (element-type 't)
				  initial-element &allow-other-keys)
  (let ((vector (make-queue-vector size element-type initial-element)))
    (apply #'call-next-method q (list* :vector vector initargs))))

;;  Enqueue

(defmethod queue-full ((q queue))
  nil)

(defmethod queue-full ((q queue-blocking-write))
  (with-accessors ((lock queue-lock)
		   (blocking-write-cv queue-blocking-write-cv)) q
    (condition-wait blocking-write-cv lock)
    t))

(defmethod on-enqueue ((q queue))
  nil)

(defmethod on-enqueue ((q queue-blocking-read))
  (condition-notify (queue-blocking-read-cv q)))

(defmethod enqueue ((q queue) item &optional blocking)
  (assert (typep item (array-element-type (queue-vector q))))
  (with-accessors ((vector queue-vector)
		   (write-index queue-write-index)
		   (length queue-length)
		   (lock queue-lock)) q
    (with-lock-held (lock)
      (labels ((write-queue ()
		 (let ((vector-length (length vector))
		       (len length))
		   (cond ((= len vector-length)
			  (when (and blocking (queue-full q))
			    (write-queue)))
			 ((< len vector-length)
			  (setf (aref vector write-index) item
				write-index (mod (1+ write-index) vector-length)
				length (1+ len))
			  (on-enqueue q)
			  t)
			 (t
			  (error "Invalid queue length"))))))
	(write-queue)))))

;;  Dequeue

(defmethod queue-empty ((q queue))
  nil)

(defmethod queue-empty ((q queue-blocking-read))
  (with-accessors ((lock queue-lock)
		   (blocking-read-cv queue-blocking-read-cv)) q
    (condition-wait blocking-read-cv lock)
    t))

(defmethod on-dequeue ((q queue))
  nil)

(defmethod on-dequeue ((q queue-blocking-write))
  (condition-notify (queue-blocking-write-cv q)))

(defmethod dequeue ((q queue) &optional blocking)
  (with-accessors ((vector queue-vector)
		   (read-index queue-read-index)
		   (length queue-length)
		   (lock queue-lock)) q
    (with-lock-held (lock)
      (labels ((read-queue ()
		 (let ((vector-length (length vector))
		       (len length))
		   (cond ((= 0 len)
			  (if (and blocking (queue-empty q))
			      (read-queue)
			      (values nil nil)))
			 ((< 0 len)
			  (let ((item (aref vector read-index)))
			    (setf read-index (mod (1+ read-index) vector-length)
				  length (1- len))
			    (on-dequeue q)
			    (values item t)))
			 (t
			  (error "Invalid queue length"))))))
	(read-queue)))))

(defmethod dequeue-all ((q queue))
  (with-accessors ((vector queue-vector)
		   (read-index queue-read-index)
		   (length queue-length)
		   (lock queue-lock)) q
    (with-lock-held (lock)
      (let* ((vector-length (length vector))
	     (len length)
	     (result (make-array `(len)
				 :element-type (array-element-type vector))))
	(dotimes (i len)
	  (setf read-index (mod (1+ read-index) vector-length)
		(aref result i) (aref vector read-index)))
	(decf length len)
	result))))

#+nil
(untrace enqueue dequeue on-enqueue on-dequeue queue-full queue-empty)
