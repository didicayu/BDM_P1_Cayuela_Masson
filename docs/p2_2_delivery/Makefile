TEX = src/p2_2_delivery.tex
OUT = build

.PHONY: all rebuild clean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=$(OUT) $(TEX)

rebuild: clean all

clean:
	rm -rf $(OUT)
