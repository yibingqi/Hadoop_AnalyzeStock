# Hadoop_AnalyzeStock
#### Analyze daily values of stock prices of major companies.

1. The program accepts 6 positional arguments.
   * The first two arguments are the start and end dates of our interval of interest, which is specified by user. The date can be in MM/DD/YYYY format. You do NOT have to do date validation or user input error check.
   * The third argument is either "avg", "min" or "max", which is an aggregation operator on the data obtained for the interval. 
   * The fourth argument is the name of the value field we want to aggregate. The argument value should be either: closing price (close), lowest price of the day(low), highest price of the day (high). See the specifications below for the name of the input fields.
   * The fifth argument is the name or the input file in HDFS.
   * The sixth argument is the HDFS path for the output.
2. The output have the aggregate value for each stock (i.e. each ticker).
3. The output format is tab or space-separated (your choice): ticker aggregated_value.
