# Dirty-E-Commerce-Data-Project

### Setting Up and Running the ETL Pipeline

To set up and run the ETL pipeline, follow these steps:

1. **Create a Python Virtual Environment**  
   A virtual environment ensures isolation of dependencies for your project. Open your terminal or command prompt and navigate to your project directory.  
   Use the following command to create a virtual environment:  
   `python -m venv venv`  
   Once created, activate the virtual environment.  
   - On Windows, run: `venv\Scripts\activate`  
   - On Linux/macOS, run: `source venv/bin/activate`

2. **Install Dependencies**  
   Ensure the virtual environment is activated, then install the required Python libraries using the `requirements.txt` file.  
   Run: `pip install -r requirements.txt`

3. **Run the ETL Pipeline**  
   Make sure the virtual environment is still activated. To execute the `etl_pipeline.py` script, Run:  `python etl_pipeline.py`

4. **Deactivate the Virtual Environment**  
   After running the script, deactivate the virtual environment by running:  
   `deactivate`

Additional Notes:  
- Make sure Python 3.x is installed on your system.  
- If `requirements.txt` is missing or incomplete, you can generate it using:  
  `pip freeze > requirements.txt`  
- If you encounter dependency issues, install missing packages manually using:  
  `pip install <package_name>`  
