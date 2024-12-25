### Project Overview
This project involves cleaning and standardizing layoff data from a dataset (world_layoffs.layoffs). The primary objective is to create a clean, standardized, and structured version of the data for further analysis. Data cleaning is a crucial step in ensuring the accuracy and usability of the dataset for exploratory data analysis (EDA) and visualization.

### Steps and Procedures
Creating a Staging Table
A staging table (layoffs_staging) was created as a working copy of the raw data to ensure the integrity of the original dataset.

### Removing Duplicates
Checked for duplicate rows using ROW_NUMBER() and removed them while retaining one legitimate record for each duplicate group.

### Standardizing Data
Null and blank values were standardized. Blank fields were converted to NULL for easier handling.
Used SQL joins to populate missing values for industry based on other rows with the same company.

### Correcting Variations in Data
Standardized variations in categorical fields like industry (e.g., multiple forms of "Crypto").
Removed trailing periods and fixed formatting inconsistencies in country fields

### Handling Date Columns
Converted date fields to a standard DATE format using STR_TO_DATE and updated the column datatype

### Analyzing and Retaining Null Values
Examined null values in critical columns such as total_laid_off and percentage_laid_off. These were left intact unless they provided no usable information.

### Removing Useless Rows
Deleted rows where both total_laid_off and percentage_laid_off were NULL, as they lacked actionable data.

### Final Cleanup
Dropped the helper column (row_num) used during duplicate removal.
Validated the dataset to ensure no redundant or incorrect data remained.

### Final Output
The cleaned dataset (world_layoffs.layoffs_staging2) is ready for further exploratory data analysis or visualization. The structured and standardized data allows for accurate insights into layoff trends and patterns across industries.

### Future Work
Perform exploratory data analysis (EDA) using tools like Tableau or Power BI.
Integrate machine learning models for predictive analysis, such as predicting future layoffs based on trends.
