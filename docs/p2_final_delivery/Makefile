DOC=p2_final_delivery

.PHONY: all rebuild clean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=build src/$(DOC).tex

rebuild: clean all

clean:
	rm -rf build/*

