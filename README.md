# HDX API Python ETL Pipeline

This repository contains a Python ETL (Extract, Transform, Load) files designed to collect data from Humanitarian Data Exchange (HDX) API. The collected data is then transformed and organized into specific directories, which are made available in the `gbHumanitarian` folder in the [geoBoundaries repository](https://github.com/wmgeolab/geoBoundaries).

## Overview

The ETL pipeline consists of the following main steps:

1. **Extraction**:
   - Data is collected from the HDX API using Python scripts and the HDX library.
   - The collected data includes various datasets related to humanitarian data.

2. **Transformation**:
   - The raw data is processed and transformed into a format compatible with the `gbHumanitarian` folder structure.
   - Data cleaning and formatting are performed to ensure consistency and usability.

3. **Loading**:
   - The transformed data is organized into specific directories and made available in the appropriate folder in the geoBoundaries repository.
   - Each dataset is stored in a structured manner for easy access and retrieval.


For more information on the geoBoundaries project, visit [wmgeolab/geoBoundaries](https://github.com/wmgeolab/geoBoundaries).
