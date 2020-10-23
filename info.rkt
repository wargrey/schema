#lang info

(define collection 'use-pkg-name)
(define pkg-desc "A data modeling tool for Typed Racket")
(define pkg-authors '(wargrey))

(define version "1.0")
(define deps '("base" "db-lib" "typed-racket-lib" "typed-racket-more"))
(define build-deps '("scribble-lib" "racket-doc"))
(define test-omit-paths 'all)

(define scribblings '(["tamer/schema.scrbl" (main-doc) ("Databases")]))
