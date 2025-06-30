import re


def clean_string(column):
    return f"ltrim(replaceRegexpAll({column}, '[^A-Za-z0-9[:space:],.\";:!?()_&%$#@]', ''))"


def capitalized_case(column):
    return f"initcapUTF8(lower({column}))"


def upper_case(column):
    return f"UPPER({column})"


def apply_cleaner(columns):
    result = []
    column_name = ''
    for row in columns:
        column_name = row[0]
        col_name = str(row[0])
        data_type = str(row[1])

        # Skip password column
        if 'password' in col_name.lower():
            continue

        # Do Clean string
        if data_type == 'String':
            column_name = clean_string(column_name)

        # Do capitalize
        if 'name' in col_name.lower():
            column_name = capitalized_case(column_name)

        # add suffix '_amount' in column name 
        if 'amount' not in col_name.lower() and 'Decimal' in data_type:
            column_name = f'{column_name} AS {col_name}_amount'

        # change column name '_at' or '_date' to '_time' if type DateTime64
        elif 'DateTime64' in data_type:
            col_name = re.sub(r'_(at|date)', '_time', col_name)
            column_name = f'{column_name} AS {col_name}'

        # Add suffix AS column_name
        else:
            column_name = f'{column_name} AS {col_name}'

        result.append(column_name)

    return result
