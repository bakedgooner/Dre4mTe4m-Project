from ipywidgets import widgets as w
from IPython.display import display
#from sidecar import Sidecar as sc

user_out = w.Output(layout={'border': '1px solid black'})
user_out

# Requirement: A user can upload a file to be inserted into the database
file_upload = w.FileUpload(
    accept='',  # Accepted file extension e.g. '.txt', '.pdf', 'image/*', 'image/*,.pdf'
    multiple=False  # True to accept multiple files upload else False
)

# Single insert
single_insert = w.Button(description="Single Insert")
# Multiple row insert
multiple_row_insert = w.Button(description="Multiple Row Insert")
# Load data syntax
load_data_syntax = w.Button(description="Load Data Syntax")


def _on_single_insert_clicked(single_insert):
    user_file = file_upload.value
    print(user_file)
    with user_out:
        print("Single insert clicked")
        print(user_file)
        #with open("input.txt", "w+b") as i:
        #    i.write("Just doing nothing"
single_insert.on_click(_on_single_insert_clicked)

def _on_multiple_row_insert_clicked(multiple_row_insert):
    user_file = file_upload.value
    with user_out:
        print("Mult row clicked")
        print(user_file)
multiple_row_insert.on_click(_on_multiple_row_insert_clicked)

def _on_load_data_syntax_clicked(load_data_syntax):
    user_file = file_upload.value
    with user_out:
        print("Load data syntax clicked")
        print(user_file)
load_data_syntax.on_click(_on_load_data_syntax_clicked)

#display(file_upload)
#display(single_insert)

# Tables (remove, retrieve, average)
table = w.Text(description="Table: ")
remove_table = w.Button(description="Remove Table")
retrieve_table = w.Button(description="Retrieve Table")
column = w.Text(description ="Column: ")
average_table_column = w.Button(description="Find Average")

user_interface = w.VBox([
    w.Label(value="Select a text file:"),
    file_upload,
    w.Label(value="Use one of the following methods to upload data:"),
    single_insert,
    multiple_row_insert,
    load_data_syntax,
    w.Label(value="Remove Data: "),
    table,
    remove_table,
    w.Label(value="Retrieve Data: "),
    table,
    retrieve_table,
    w.Label(value="Find Average: "),
    table,
    column,
    average_table_column,
])
display(user_interface)
user_out.clear_output()
display(user_out)

user_out.clear_output()
