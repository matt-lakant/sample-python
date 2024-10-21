from datetime import datetime

# Print section headings in Python terminal
def print_section_heading(heading):
    """Prints a formatted section heading."""
    separator = "#" * 100  # You can change the length of the separator as needed
    print(f"\n{separator}\n# {heading}\n{separator}\n")

def print_subsection_heading(subheading):
    """Prints a formatted sub-section heading with left indent."""
    indent=4
    separator = "#" * (80 - indent)  # Adjust the separator length based on indent
    indent_space = ' ' * indent
    print(f"\n{indent_space}{separator}\n{indent_space}## {subheading}\n{indent_space}{separator}\n")

# Converts "data_date_str" into a real date "data_date", returns False if invalid date
def valid_date(data_date_str, date_format):
    try:
        datetime.strptime(data_date_str, date_format)
        return datetime.strptime(data_date_str, date_format).date()
    except ValueError:
        print(f"{data_date_str} is NOT a valid date")
        return False