Python code to process flow logs file is in flow_logs_parser.py file. The sample lookup_table file is in the data folder.

This code will only work with the sample flow log data format provided in the email. It has to be modified if the data type or column ordering changes.

Doctest is used for testing.

There are a bunch of test flow_logs files: 

- test/flow_logs1
- test/flow_logs2
- test/flow_logs3

Test results are written to the output files (2 output files for each flow_logs file):

- test/tag_count_results1
- test/port_protocol_count_results1

- test/tag_count_results2
- test/port_protocol_count_results2

- test/tag_count_results3
- test/port_protocol_count_results3

To run the doctests from command line use:
- python -m doctest -v log_parser.py
