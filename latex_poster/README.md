# LaTeX Poster for INCF 2025

This directory contains a LaTeX version of your research transparency poster for the INCF 2025 conference.

## Requirements

To compile this poster, you'll need:

1. A LaTeX distribution (e.g., TeX Live, MiKTeX)
2. The `baposter` class (usually included in modern LaTeX distributions)
3. Required packages (most are standard):
   - tikz and pgfplots (for charts)
   - multicol (for multi-column layout)
   - booktabs (for nice tables)
   - Other standard packages listed in the preamble

## Compilation

### Using Make (Linux/Mac)
```bash
make              # Compile the poster
make clean        # Remove auxiliary files
make cleanall     # Remove all generated files including PDF
make view         # Compile and view the PDF
```

### Manual Compilation
```bash
pdflatex poster.tex
pdflatex poster.tex  # Run twice for proper layout
```

### Using Overleaf

1. Create a new project on Overleaf
2. Upload `poster.tex`
3. Compile directly on Overleaf

## Structure

The poster is organized into the following sections:

1. **Header**: Title, authors, and affiliations
2. **Introduction**: Background and objectives
3. **Methods**: Data processing pipeline
4. **Results**: Main findings with charts
5. **Analysis**: Detailed transparency indicators
6. **Implementation**: Tools developed
7. **Discussion**: Key insights and future directions
8. **Footer**: Acknowledgments and references

## Customization

### Changing Content
- All text content is in `poster.tex`
- Modify text directly in each `\headerbox{}`

### Adding Figures
```latex
\includegraphics[width=0.8\textwidth]{figures/your_figure.pdf}
```

### Modifying Charts
- Bar charts and trend plots use TikZ/pgfplots
- Data points are directly in the tex file
- Modify coordinates to update data

### Changing Colors
Colors are defined at the top of the file:
```latex
\definecolor{lightblue}{rgb}{0.145,0.6,0.945}
\definecolor{darkblue}{rgb}{0.0,0.2,0.4}
```

### Adjusting Layout
- Change `span=` values in headerbox to adjust column widths
- Modify `below=` to change vertical arrangement
- Adjust `colspacing` and other parameters in poster options

## Adding Real Data

Replace placeholder data in the charts:
1. In the funder comparison bar chart, update the coordinates
2. In the trend chart, replace with actual yearly percentages
3. Update the coverage table with real statistics

## Fonts

The poster uses Times font (`\usepackage{times}`). You can change this by replacing with other font packages.

## Troubleshooting

1. **Missing baposter class**: Install texlive-latex-extra or download from CTAN
2. **Compilation errors**: Make sure all packages are installed
3. **Layout issues**: Run pdflatex twice to resolve references

## Tips for Overleaf

- The poster should compile without issues on Overleaf
- Use "Download as ZIP" to get all files including the compiled PDF
- Set compiler to pdfLaTeX in project settings