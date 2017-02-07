#lang typed/racket/base

(provide (except-out (all-defined-out) variant+clock-sequence))
(provide (all-from-out typed/racket/random))

(require racket/fixnum)
(require racket/flonum)
(require racket/math)

(require typed/racket/date)
(require typed/racket/random)

(define utc-seconds : (->* (Index) (Positive-Byte Positive-Byte Byte Byte Byte Boolean) Integer)
  (lambda [year [month 1] [day 1] [hour 0] [minute 0] [second 0] [local? #true]]
    (find-seconds second minute hour day month year local?)))

(define pk64:timestamp : (->* () (Integer) Integer)
  (lambda [[diff:s 0]]
    (define version : Byte 1) #b11
    (define now:ms : Flonum (current-inexact-milliseconds))
    (define now:s : Integer (fxquotient (exact-floor now:ms) 1000))
    (define diff32:s : Fixnum (fxand (fx- now:s diff:s) #xFFFF))
    (define us : Fixnum (fx- (exact-floor (fl* now:ms 1000.0)) (fx* now:s 1000000)))
    (bitwise-ior (arithmetic-shift diff32:s 32)
                 (arithmetic-shift version 30)
                 (fxlshift us 14)
                 (variant+clock-sequence))))

(define pk64:random : (->* () (Integer) Integer)
  (lambda [[diff:s 0]]
    (define version : Byte 2) #b11
    (define diff32:s : Fixnum (fxand (fx- (current-seconds) diff:s) #xFFFF))
    (define urnd : Integer (integer-bytes->integer (crypto-random-bytes 2) #false #true))
    (bitwise-ior (arithmetic-shift diff32:s 32)
                 (arithmetic-shift version 30)
                 (fxlshift (variant+clock-sequence) 16)
                 (fxand urnd #xFFFF))))

(define variant+clock-sequence : (-> Index)
  (lambda [] ; TODO: what if the clock is set backwards?
    (fxand (current-memory-use) #b11111111111111)))
