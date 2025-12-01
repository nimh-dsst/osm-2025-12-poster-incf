# Makefile for LaTeX poster compilation

MAIN = poster
LATEX = pdflatex
BIBTEX = bibtex
RM = rm -f

# Main target
all: $(MAIN).pdf

# Compile the poster
$(MAIN).pdf: $(MAIN).tex
	$(LATEX) $(MAIN)
	$(LATEX) $(MAIN)

# Clean auxiliary files
clean:
	$(RM) $(MAIN).aux $(MAIN).log $(MAIN).out $(MAIN).nav $(MAIN).snm $(MAIN).toc $(MAIN).vrb
	$(RM) *.aux *.log *.out *.nav *.snm *.toc *.vrb

# Clean everything including PDF
cleanall: clean
	$(RM) $(MAIN).pdf

# View the PDF (Linux)
view: $(MAIN).pdf
	xdg-open $(MAIN).pdf &

# Continuous compilation (requires latexmk)
watch:
	latexmk -pdf -pvc $(MAIN).tex

.PHONY: all clean cleanall view watch