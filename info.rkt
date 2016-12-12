#lang info

(define collection 'multi)
(define pkg-desc "A data modeling tool (along with the lightweight and elegant data access layer) for Typed Racket")
(define pkg-authors '(wargrey))

(define version "1.0")
(define deps '("base" "digimon" "db-lib" "typed-racket-lib" "typed-racket-more"))
(define build-deps '("scribble-lib" "racket-doc"))

(define scribblings '(["tamer/schema.scrbl" (main-doc) ("Databases")]))
