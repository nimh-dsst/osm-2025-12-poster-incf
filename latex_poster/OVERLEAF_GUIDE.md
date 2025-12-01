# Overleaf Upload Guide

This guide will help you upload and work with the LaTeX poster on Overleaf.

## Quick Start

1. **Create New Project on Overleaf**
   - Go to [Overleaf](https://www.overleaf.com)
   - Click "New Project" → "Blank Project"
   - Name it "INCF 2025 Poster"

2. **Upload Files**
   - Delete the default `main.tex` file
   - Upload `poster.tex` (or `poster_modular.tex` for the enhanced version)
   - Create a `figures` folder if you have images to include

3. **Compile**
   - The poster should compile automatically
   - Make sure the compiler is set to "pdfLaTeX" (usually default)

## File Descriptions

- **poster.tex**: Basic version matching your Google Slides design
- **poster_modular.tex**: Enhanced version with:
  - Better code organization
  - Easy-to-update data sections
  - More customization options
  - Cleaner styling

## Customization Tips

### Updating Data

In `poster_modular.tex`, data is defined at the top for easy updates:

```latex
% Update these with your actual data
\def\funderPercentages{16.16,16.72,20.14,22.78,23.17,23.58,24.20,25.20,26.76,45.61}
\def\trendValues{2.5,2.8,3.2,3.8,4.5,5.2,6.8,8.5,11.2,14.8,19.5,24.2,28.5,31.8,33.2}
```

### Adding Images

1. Create a folder called `figures` in Overleaf
2. Upload your images to this folder
3. Include them in your poster:
```latex
\includegraphics[width=0.8\textwidth]{figures/your_image.pdf}
```

### Changing Colors

Colors are defined at the top:
```latex
\definecolor{primaryColor}{rgb}{0.145,0.6,0.945}    % Light blue
\definecolor{secondaryColor}{rgb}{0.0,0.2,0.4}      % Dark blue
\definecolor{accentColor}{rgb}{0.9,0.4,0.1}         % Orange
```

### Adding Logos

For institutional logos in the header:
```latex
% In the eye catcher section (first empty braces after \begin{poster})
\includegraphics[height=8em]{figures/university_logo.pdf}

% For conference logo (last empty braces in the title section)
\includegraphics[height=8em]{figures/conference_logo.pdf}
```

## Common Issues and Solutions

### Issue: "File 'baposter.cls' not found"
**Solution**: This shouldn't happen on Overleaf, but if it does, download baposter.cls from CTAN and upload it to your project.

### Issue: Figures not showing
**Solution**:
- Check file names are correct (case-sensitive)
- Ensure figures are in the `figures/` folder
- Try different formats (PDF usually works best)

### Issue: Text overflow in boxes
**Solution**:
- Reduce font size locally: `{\small Your text here}`
- Adjust column spacing
- Move content to another box

## Export Options

1. **Download PDF**: Click "Download PDF" button
2. **Download Source**: Menu → Download → Source
3. **Print Size**: The poster is A0 size (841 × 1189 mm or 33.1 × 46.8 inches)

## Collaboration

- Use "Share" button to collaborate with co-authors
- Overleaf tracks changes automatically
- Use comments for discussions

## Performance Tips

- For large posters, compilation might be slow
- Use "Draft mode" while editing (faster compilation)
- Switch to "Normal mode" for final version

## Final Checklist

Before submitting:
- [ ] All data is updated with actual values
- [ ] All author names and affiliations are correct
- [ ] Figures are high quality (300 DPI for raster images)
- [ ] No overfull boxes (text overflow)
- [ ] Colors are accessible (good contrast)
- [ ] Email and links are correct
- [ ] Funding acknowledgments are complete