"""
Tara Dixit
Code to parse a log file, map attributes to a lookup table and write results to files
"""

lookup_table_name = 'data/lookup_table'

flow_logs_table_name = 'test/flow_logs1'
flow_logs_table_name_test1 = 'test/flow_logs2'
flow_logs_table_name_test2 = 'test/flow_logs3'

tag_count_results = 'test/tag_count_results1'
port_protocol_count_results = 'test/port_protocol_count_results1'
tag_count_results1 = 'test/tag_count_results2'
port_protocol_count_results1 = 'test/port_protocol_count_results2'
tag_count_results2 = 'test/tag_count_results3'
port_protocol_count_results2 = 'test/port_protocol_count_results3'

# column 6 contains the destination port in the sample flow logs file
dstport_col_num = 6

untagged_key = 'Untagged'
protocol_key = 'protocol'
tag_key = 'tag'

# data dictionary to keep the lookup table
lookup_dict = {}

def process_flow_logs(input_flow_logs, input_tag_count_results, input_port_protocol_count_results):
    """
        Method to process the flow logs.
    """

    # read the lookup file and populate its content in a data dictionary
    populate_lookup_table()

    # data dictionaries to hold the two results and write them in the results file later
    tag_count = {}
    port_protocol_count = {}

    # read the flow logs file
    with open(input_flow_logs, 'r') as file:
        for line in file:
            parts = line.strip().split(' ')
            dstport_val = parts[dstport_col_num]
            if dstport_val in lookup_dict:
                value = lookup_dict[dstport_val]
                if value[protocol_key].lower() in tag_count:
                    tag_count[value[protocol_key]] += 1
                else:
                    tag_count[value[protocol_key]] = 1
                if dstport_val.lower() in port_protocol_count:
                    port_protocol_count[dstport_val][value[protocol_key]] += 1
                else:
                    temp_dict = {}
                    temp_dict[value[protocol_key]] = 1
                    port_protocol_count[dstport_val] = temp_dict
            else:
                if untagged_key.lower() in tag_count:
                    tag_count[untagged_key] += 1
                else:
                    tag_count[untagged_key] = 1

        write_results_to_file(tag_count, input_tag_count_results)
        write_results_to_file(port_protocol_count, input_port_protocol_count_results)

def populate_lookup_table():
    """
        Method to read and populate the lookup table data into a data dictionary.
    """
    with open(lookup_table_name, 'r') as f:
        # skip the first header line
        next(f)
        for line in f:
            dstport, protocol, tag = line.strip().split(',')
            lookup_dict[dstport] = {protocol_key: protocol, tag_key: tag}

def write_results_to_file(results_dict, file_name):
    """
        Method for writing the results from data dictionary to the output files.
    """

    with open(file_name, 'w') as f:
        for key, value in results_dict.items():
            if isinstance(value, dict):
                value_str = ', '.join(f"{k}, {v}" for k, v in value.items())
                f.write(f"{key}, {value_str}\n")
            else:
                f.write(f"{key}, {value}\n")

def count_lines(file_name):
    """
        Method for counting the lines in a file.
    """
    with open(file_name, 'r') as f:
        return sum(1 for _ in f)

def main():
    """
        Test different flow log analysis scenarios.

        >>> process_flow_logs(flow_logs_table_name, tag_count_results, port_protocol_count_results)
        >>> count_lines(tag_count_results)
        2
        >>> count_lines(port_protocol_count_results)
        6
        >>> process_flow_logs(flow_logs_table_name_test1, tag_count_results1, port_protocol_count_results1)
        >>> count_lines(tag_count_results1)
        1
        >>> count_lines(port_protocol_count_results1)
        1
        >>> process_flow_logs(flow_logs_table_name_test2, tag_count_results2, port_protocol_count_results2)
        >>> count_lines(tag_count_results2)
        0
        >>> count_lines(port_protocol_count_results2)
        0
    """
    process_flow_logs(flow_logs_table_name, tag_count_results, port_protocol_count_results)

if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=True)
    main()