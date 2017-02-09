#lang typed/racket/base

(provide (all-defined-out))
(provide (all-from-out typed/racket/random))

(require racket/fixnum)
(require racket/flonum)
(require racket/math)

(require typed/racket/date)
(require typed/racket/random)

(define utc-seconds : (->* (Index) (Positive-Byte Positive-Byte Byte Byte Byte Boolean) Integer)
  (lambda [year [month 1] [day 1] [hour 0] [minute 0] [second 0] [local? #true]]
    (find-seconds second minute hour day month year local?)))

;;; WARNING
; The actual generated 64bit primary keys only have 63 significant bits,
;  since integers in SQLite3 are signed and the overflowed ones will be converted to `real`s unrecognized,
;  nonetheless, 63bit is still reasonably good enough.

(define pk64:timestamp : (->* () (Integer) Integer)
  (lambda [[diff:s 0]]
    (define version : Byte #b01)
    (define now:ms : Flonum (current-inexact-milliseconds))
    (define now:s : Integer (fxquotient (exact-floor now:ms) 1000))
    (define ts32 : Fixnum (fxand (fx- now:s diff:s) #x7FFF))
    (define us16 : Fixnum (fx- (exact-floor (fl* now:ms 1000.0)) (fx* now:s 1000000)))
    (define random-clock14 : Integer (fxand (current-memory-use) #b11111111111111))
    (bitwise-ior (arithmetic-shift ts32 32)
                 (arithmetic-shift version 30)
                 (fxlshift us16 14)
                 random-clock14)))

(define pk64:random : (->* () () Integer)
  (lambda []
    (define version : Byte #b10)
    (define ts32 : Integer (bitwise-and (integer-bytes->integer (crypto-random-bytes 4) #false #true) #x7FFF))
    (define urnd16 : Integer (integer-bytes->integer (crypto-random-bytes 2) #false #true))
    (define clock14 : Integer (random (fx+ #b11111111111111 1)))
    (bitwise-ior (arithmetic-shift ts32 32)
                 (arithmetic-shift version 30)
                 (fxlshift urnd16 14)
                 clock14)))
