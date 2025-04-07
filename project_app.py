from customtkinter import *
import subprocess
import threading
import tkinter as tk
import sys
import io
from PIL import Image

def run_import_script():
    def stream_output():
        process = subprocess.Popen(
            ["python", "combined_import_script.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        for line in process.stdout:
            output_box.insert("end", line)
            output_box.see("end")

    # Run in a thread to keep UI responsive
    thread = threading.Thread(target=stream_output)
    thread.start()

def run_query_time_script():
    try:
        value = int(number_input.get())
        if value < 5 or value > 100:
            output_textbox.insert("end", "Please enter a number between 5 and 100.\n")
            return
    except ValueError:
        output_textbox.insert("end", "Invalid input. Please enter a valid number.\n")
        return

    output_textbox.delete("1.0", "end")  # Clear old output

    def stream_output():
        try:
            process = subprocess.Popen(
                ["python", "query_time.py", str(value)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            for line in process.stdout:
                output_textbox.insert("end", line)
                output_textbox.see("end")
            process.stdout.close()
            process.wait()
        except Exception as e:
            output_textbox.insert("end", f"Error running script: {e}\n")

    # Run in a separate thread
    thread = threading.Thread(target=stream_output)
    thread.start()

def run_pg_query_time_script():
    try:
        value = int(pg_number_input.get())
        if value < 5 or value > 100:
            pg_output_textbox.insert("end", "Please enter a number between 5 and 100.\n")
            return
    except ValueError:
        pg_output_textbox.insert("end", "Invalid input. Please enter a valid number.\n")
        return

    pg_output_textbox.delete("1.0", "end")  # Clear old output

    def stream_output():
        try:
            process = subprocess.Popen(
                ["python", "pg_query_time.py", str(value)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            for line in process.stdout:
                pg_output_textbox.insert("end", line)
                pg_output_textbox.see("end")
            process.stdout.close()
            process.wait()
        except Exception as e:
            pg_output_textbox.insert("end", f"Error running script: {e}\n")

    # Run in a separate thread
    thread = threading.Thread(target=stream_output)
    thread.start()

def run_mongo_query_time_script():
    try:
        value = int(m_number_input.get())
        if value < 1 or value > 20:
            m_output_textbox.insert("end", "Please enter a number between 5 and 100.\n")
            return
    except ValueError:
        m_output_textbox.insert("end", "Invalid input. Please enter a valid number.\n")
        return

    m_output_textbox.delete("1.0", "end")  # Clear old output

    def stream_output():
        try:
            process = subprocess.Popen(
                ["python", "mongo_query_time.py", str(value)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            for line in process.stdout:
                m_output_textbox.insert("end", line)
                m_output_textbox.see("end")
            process.stdout.close()
            process.wait()
        except Exception as e:
            m_output_textbox.insert("end", f"Error running script: {e}\n")

    # Run in a separate thread
    thread = threading.Thread(target=stream_output)
    thread.start()

def capture_output():
    # Redirect stdout
    old_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()

    try:
        subprocess.run(["python", "query_time.py", str(number_input.get())], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    finally:
        sys.stdout = old_stdout
        output = buffer.getvalue()
        output_textbox.insert("end", output)

def check_all_checked():
    if all(var.get() == 1 for var in check_vars):
        import_button.configure(state="normal")
    else:
        import_button.configure(state="disabled")

# Function to update the image when selection changes
def update_image(selection):
    image_paths = {
        "Compare all": "averages_comparison.png",
        "MySQL": "mysql_execution_plot.png",
        "PostgreSQL": "pg_execution_plot.png",
        "MongoDB": "mongo_execution_plot.png",
        "MongoDB-NoJoin": "mongo_execution_nj_plot.png"
    }

    try:
        image = CTkImage(
            light_image=Image.open(image_paths[selection]),
            size=(800, 500)  # adjust this size to your actual images
        )
        image_label.configure(image=image, text="")
        image_label.image = image  # Keep reference
    except FileNotFoundError:
        image_label.configure(image=None, text=f"Image not found for '{selection}'")

# Redirect print to CTkTextbox
class TextRedirector:
    def __init__(self, textbox):
        self.textbox = textbox

    def write(self, string):
        self.textbox.insert("end", string)
        self.textbox.see("end")

    def flush(self):
        pass  # Required for compatibility with sys.stdout

# initialize the gui
app = CTk()
app.geometry("1200x700")
app.title("DS7330 Project Application")
set_appearance_mode("dark")

# add tabs
tabview = CTkTabview(master=app)
tabview.pack(padx=30, pady=10, fill="both", expand=True)

tabview.add("Data Import") # checklist for app and data import
tabview.add("MySQL") # MySQL Query Time
tabview.add("PostgreSQL") # PostgreSQL Query Time
tabview.add("MongoDB") # MongoDB Query Time
tabview.add("Comparisons") # Comparison

# Data Import
# disclaimer for those that already have data imported
skip_label = CTkLabel(tabview.tab("Data Import"), text="Skip this tab if successfully imported data previously", font=("Arial", 22, "bold"))
skip_label.pack(padx=10, pady=10, fill="both", expand=True)

# frame for checklist
description_frame = CTkFrame(tabview.tab("Data Import"), fg_color="#8D6F3A", border_color="#FFCC70", border_width=2)
description_frame.pack(padx=50, pady=10, fill="both", expand=True)

# checklist
descriptions = [
    "MySQL: Create a database on your MySQL server and enable LOCAL INFILE (run create_database.sql)",
    "PostgreSQL: Create a database on your PostgreSQL server",
    "MongoDB: Create a connection on your MongoDB",
    "Update the MySQL, PostgreSQL, and MongoDB config files (db_config, pg_db_config, and mongo_db_config respectively)",
    "If you have run the import script before and it failed, please REFRESH and DROP/DELETE all the tables from your database(s)"
]

check_vars = []

# loops through each point on checklist
for description in descriptions:
    var = tk.IntVar()
    check_vars.append(var)

    check_box = CTkCheckBox(description_frame, text=description, variable=var, command=check_all_checked)
    check_box.pack(anchor="w", padx=10, pady=5)

# frame for button
button_frame = CTkFrame(tabview.tab("Data Import"), height = 40)
button_frame.pack(padx=10, pady=10, fill="both", expand=True)

# button to run import script, disabled by default until checklist complete
import_button = CTkButton(button_frame, text="Run Import Script", state="disabled", command=run_import_script)
import_button.pack(pady=10)

# text output of print statements for debugging
output_frame = CTkFrame(tabview.tab("Data Import"))
output_frame.pack(padx=50, pady=10, fill="both", expand=True)

# Textbox to show output
output_box = CTkTextbox(output_frame, height=250)
output_box.pack(fill="both", expand=True, padx=10, pady=10)

# Redirect stdout
sys.stdout = TextRedirector(output_box)

# MySQL
input_label = CTkLabel(tabview.tab("MySQL"), text = "Enter number of iterations to average (5-100):")
input_label.pack(pady=(20, 5))

# input for iterations to average on
number_input = CTkEntry(tabview.tab("MySQL"))
number_input.pack(pady=5)

# Output textbox
output_textbox = CTkTextbox(tabview.tab("MySQL"), height=200)
output_textbox.pack(pady=20, fill="both", expand=True)

# Button to run script
run_button = CTkButton(tabview.tab("MySQL"), text="Run query_time.py", command=run_query_time_script)
run_button.pack(pady=10)

# PostgreSQL
pg_input_label = CTkLabel(tabview.tab("PostgreSQL"), text = "Enter number of iterations to average (5-100):")
pg_input_label.pack(pady=(20, 5))

# input for iterations to average on
pg_number_input = CTkEntry(tabview.tab("PostgreSQL"))
pg_number_input.pack(pady=5)

# Output textbox
pg_output_textbox = CTkTextbox(tabview.tab("PostgreSQL"), height=200)
pg_output_textbox.pack(pady=20, fill="both", expand=True)

# Button to run script
pg_run_button = CTkButton(tabview.tab("PostgreSQL"), text="Run pg_query_time.py", command=run_pg_query_time_script)
pg_run_button.pack(pady=10)

# MongoDB
m_input_label = CTkLabel(tabview.tab("MongoDB"), text = "Enter number of iterations to average (1-20):")
m_input_label.pack(pady=(20, 5))

# input for iterations to average on
m_number_input = CTkEntry(tabview.tab("MongoDB"))
m_number_input.pack(pady=5)

# Output textbox
m_output_textbox = CTkTextbox(tabview.tab("MongoDB"), height=200)
m_output_textbox.pack(pady=20, fill="both", expand=True)

# Button to run script
m_run_button = CTkButton(tabview.tab("MongoDB"), text="Run mongo_query_time.py", command=run_mongo_query_time_script)
m_run_button.pack(pady=10)

# Comparisons

comparisons_frame = CTkFrame(tabview.tab("Comparisons"))
comparisons_frame.pack(expand=True, fill="both", padx=20, pady=20)

options = ["Compare all", "MySQL", "PostgreSQL", "MongoDB", "MongoDB-NoJoin"]

# Image label placeholder
image_label = CTkLabel(comparisons_frame, text="")
image_label.grid(row=0, column=1, padx=20, pady=20, sticky="e")

# Dropdown menu
dropdown = CTkOptionMenu(comparisons_frame, values=options, command=update_image)
dropdown.set("Compare all")
dropdown.grid(row=0, column=0, padx=20, pady=20, sticky="nw")

# Load default image
update_image("Compare all")

# runs the app
app.mainloop()