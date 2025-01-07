# High-Speed Fuzzy String Matching with RapidFuzz and Multiprocess for Big Data

String matching in massive datasets can be painfully slow and inefficient when using traditional methods. This project addresses the problem by leveraging RapidFuzz for fuzzy matching, Multiprocess for parallelized processing, and PySpark for handling big data seamlessly. The result is a high-speed solution designed to efficiently process and match millions of records.

## Problems
- Traditional string matching methods are often too slow for large datasets, especially when handling millions of records.
- Without parallel processing, matching tasks become a bottleneck, making real-time or near-real-time analysis impractical.
- Processing large-scale data requires a robust framework that can handle the computational and memory challenges efficiently.

## Features
- High-Speed Matching: Achieve faster processing using Multiprocess for parallelized execution.
- Accurate Fuzzy Matching: Utilize RapidFuzz, a robust library for string similarity measurement.
- Big Data Ready: Incorporates PySpark for scalable data processing and handling datasets beyond memory limits.
- Customizable Threshold: Set your own similarity score threshold to control match precision.
- Detailed Reporting: Outputs results as a well-structured Excel file for easy analysis.

## Dependencies
The project uses the following key Python libraries:
- `pyspark`
- `findspark`
- `pandas`
- `rapidfuzz`
- `multiprocessing`

Ensure all dependencies are installed before running the project.

## Setup Instructions
1. Clone this repository:
  ```bash
  git clone https://github.com/fast-string-matching-with-large-dataset.git
  cd fast-string-matching-with-large-dataset
  ```
2. Install dependencies:
   ```bash
   pip install pyspark findspark pandas rapidfuzz multiprocess
   ```
3. Configure your Spark environment as needed.
4. Update SQL queries in the script to match your data source schemas.
5. Run the script:
   ```bash
   python string_matching.py
   ```
