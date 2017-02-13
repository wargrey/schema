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
; The actual generated 64bit primary keys only have 63 significant bits (3bits therefore are used to represent `version`),
;  since integers in SQLite3 are signed and the overflowed ones will be converted to `real`s unexpectedly.

(define pk64:timestamp : (->* () (Integer) Integer)
  (lambda [[diff:s 0]]
    (define version : Byte #b001)
    (define now:ms : Flonum (current-inexact-milliseconds))
    (define ts32 : Fixnum (fxand (fx- (fxquotient (exact-round now:ms) 1000) diff:s) #xFFFF))
    (define ms20 : Fixnum (fxremainder (exact-round (fl* now:ms 1000.0)) 1000000))
    (define clock-seq8 : Fixnum (fxremainder (current-memory-use) #xFF))
    (bitwise-ior (arithmetic-shift version 60)
                 (arithmetic-shift ts32 28)
                 (fxlshift ms20 8)
                 clock-seq8)))

(define pk64:random : (->* () () Integer)
  (lambda []
    (define version : Byte #b100)
    (define ts32 : Integer (integer-bytes->integer (crypto-random-bytes 4) #false #true))
    (define urnd28:16 : Integer (integer-bytes->integer (crypto-random-bytes 2) #false #true))
    (define urnd28:12 : Integer (random (fx+ #xFFF 1)))
    (define clock-seq4 : Fixnum (random (fx+ #b1111 1)))
    (bitwise-ior (arithmetic-shift version 60)
                 (arithmetic-shift ts32 28)
                 (fxlshift urnd28:16 12)
                 (fxlshift urnd28:12 4)
                 clock-seq4)))
