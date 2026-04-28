PROJECT := p2_1_delivery
SRC_DIR := src
BUILD_DIR := build
MAIN_TEX := $(SRC_DIR)/$(PROJECT).tex
OUTPUT_PDF := $(BUILD_DIR)/$(PROJECT).pdf
LATEXMK ?= latexmk
LATEXMK_FLAGS := -pdf -interaction=nonstopmode -halt-on-error -file-line-error -outdir=$(BUILD_DIR)

.PHONY: all clean distclean rebuild

all: $(OUTPUT_PDF)

$(OUTPUT_PDF): $(MAIN_TEX) | $(BUILD_DIR)
	$(LATEXMK) $(LATEXMK_FLAGS) $(MAIN_TEX)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	$(LATEXMK) -C -outdir=$(BUILD_DIR) $(MAIN_TEX) || true
	touch $(BUILD_DIR)/.gitkeep

distclean: clean
	find $(BUILD_DIR) -type f ! -name '.gitkeep' -delete

rebuild: distclean all
