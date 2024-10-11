import random
import os
import names
import uuid
import shutil
import calendar
import operator
import logging
import re
from unidecode import unidecode
import pickle as pk
from datetime import datetime as dt

class EmployeeDatabase:

    """
    A class to manage an employee database

    This class provides methods to initialize, load, update, and manage employee records in a database file
    It supports operations such as adding employees, generating metadata, and restoring from backups

    """
    
    DEFAULT_MAX_EMPLOYEES = 9999
    DEFAULT_COMPANY_NAME = "Company Name"
    
    def __init__(self, file_path: str, company_name: str = None, email_suffix: str = None, max_employees: int = None):

        """
        Initializes an EmployeeDatabase instance with a specified file path and an optional maximum employee limit

        Args:
            file_path (str): The path to the database file
            company_name (str, optional): The name of the company associated with the database. Defaults to DEFAULT_COMPANY_NAME
            email_suffix (str, optional): The email suffix used for employee email addresses. Defaults to None
            max_employees (int, optional): The maximum number of employees allowed. Defaults to DEFAULT_MAX_EMPLOYEES

        Attributes:
            FILE_PATH (str): The path to the database file
            COMPANY_NAME (str): The name of the company associated with the database
            CREATION_DATE (datetime): The date when the database was created or initialized
            MAX_EMPLOYEES (int): The maximum number of employees allowed in the database
            employees (dict): A dictionary holding employee records
        """

        self.FILE_PATH = file_path
        self.COMPANY_NAME = company_name if company_name is not None else self.DEFAULT_COMPANY_NAME
        self.CREATION_DATE = dt.now()
        self.MAX_EMPLOYEES = max_employees if max_employees is not None else self.DEFAULT_MAX_EMPLOYEES

        if email_suffix is None:
            self.EMAIL_SUFFIX = self.normalize_string(self.COMPANY_NAME)
        else:
            self.EMAIL_SUFFIX = self.normalize_string(email_suffix)

        self.employees = {}

        self._load_data()

    def normalize_string(self, str) -> str:
        """
        Normalize a string by converting it to lowercase and removing non-alphabetic characters

        Args:
            str (str): The string to be normalized
        
        Returns:
            str: The normalized string containing only lowercase alphabetic characters
        """
        ascii_filtered = unidecode(str).lower()
        return re.sub(r'[^a-z]', '', ascii_filtered)
    
    def _load_data(self) -> None:
        """
        Load the employee data from the specified database file

        This method attempts to load the employee database from a file.
        
        It performs the following steps:
        
        1. Creates a backup of the existing database file (if it exists) for recovery purposes
        2. Verifies whether the database file exists and is non-empty
        3. Attempts to load the file using pickle
            If the file is invalid or corrupted:
            - If a backup exists, attempts to restore the database from the backup
            - Logs relevant error messages and handles exceptions
        4. If the file does not exist or is empty, initializes a new database with default values
        5. Serializes and stores the initialized data in a new database file if loading fails

        Raises:
            ValueError: If the file format is invalid or missing required keys
            EOFError: If the file is unexpectedly truncated
            pk.UnpicklingError: If an error occurs while unpickling the file
        """
        
        # Create a backup in case of failure
        backup_file_path = f"{self.FILE_PATH}.bak"
        if os.path.exists(self.FILE_PATH):
            shutil.copy2(self.FILE_PATH, backup_file_path)
            logging.info(f"Backup created at {backup_file_path}.")
        
        # Verify file exists and is not empty
        if os.path.exists(self.FILE_PATH) and os.path.getsize(self.FILE_PATH) > 0:
            try:
                with open(self.FILE_PATH, 'rb') as file:
                    data = pk.load(file)
                    
                    if 'metadata' not in data or 'employees' not in data:
                        raise ValueError("Invalid database file format: Missing required keys.")
                        
                    metadata = data['metadata']
                    _creation_date = metadata['creation_date']
                    self.CREATION_DATE = dt.strptime(_creation_date, "%Y-%m-%d")
                    self.COMPANY_NAME = metadata['company_name']
                    self.EMAIL_SUFFIX = metadata['email_suffix']
                    self.MAX_EMPLOYEES = metadata['max_employees']
                    self.employees = data['employees']
                    
                    logging.info(f"Database loaded successfully from {self.FILE_PATH}")
                    return
            except (EOFError, pk.UnpicklingError) as e:
                logging.critical(f"Error loading existing database: {e}.\nWill attempt to restore from backup.")
                if self.restore_from_backup():
                    return
            except ValueError as e:
                logging.error(f"Data validation error: {e}")
            except Exception as e:
                logging.error(f"Unexpected error loading file: {e}")
                
        elif not os.path.exists(self.FILE_PATH):
            logging.warning("No database file found")
        else:
            logging.warning("Database file is empty")
                
        # Initialize to default if database does not exist or loading fails
        logging.info("Initializing empty database.")
        _metadata = self.generate_metadata()
        data = {
            'metadata': _metadata,
            'employees': {}
        }
        
        # Create or overwrite the file with the initialized data
        with open(self.FILE_PATH, 'wb') as file:
            pk.dump(data, file)
        
        logging.info(f"Initialized empty database at {self.FILE_PATH}")
    
    def generate_metadata(self) -> dict:

        """
        Generate and return metadata for the employee database

        The metadata contains key information about the database

        Returns:
            dict: A dictionary containing the following metadata:
                - "company_name" (str): The name of the company associated with the database
                - "email_suffix" (str): The email suffix used for employee email addresses
                - "creation_date" (str): The creation date of the database, formatted as "YYYY-MM-DD"
                - "max_employees" (int): The maximum number of employees allowed
                - "total_employees" (int): The current number of employees in the database
        """

        return {
            'company_name': self.COMPANY_NAME,
            'email_suffix': self.EMAIL_SUFFIX,
            'creation_date': self.CREATION_DATE.strftime("%Y-%m-%d"),
            'max_employees': self.MAX_EMPLOYEES,
            'total_employees': len(self.employees)
        }
    
    def restore_from_backup(self) -> bool:

        """
        Attempts to restore the database from a backup file

        Returns:
            bool: True if the backup restoration is successful, False otherwise

        Process:
            - If a backup file exists, it copies the backup to replace the current database file
            - Attempts to reload the database from the restored backup
            - If successful, logs a success message and returns True
            - If an error occurs during the restoration or no backup file exists, logs the issue and returns False
        """

        backup_file_path = f"{self.FILE_PATH}.bak"
        
        if os.path.exists(backup_file_path):
            try:
                shutil.copy2(backup_file_path, self.FILE_PATH)
                logging.info("Backup restored successfully.")
                self._load_data()
                return True
            except Exception as e:
                logging.error(f"Error restoring from backup: {e}")
                return False
        else:
            logging.warning("No backup file found to restore.")
            return False
    
    def _update_file(self) -> bool:

        """
        Update the employee database file with the current data

        This method serializes the current employee data and metadata to the specified file path

        Returns:
            bool: True if the file was updated successfully, False otherwise
        """

        _metadata = self.generate_metadata()
        _data = {
            'metadata': _metadata,
            'employees': self.employees
        }
        try:
            with open(self.FILE_PATH, 'wb') as file:
                pk.dump(_data, file)
            logging.info("File updated correctly.")
            return True
        except Exception as e:
            logging.error(f"Error updating file: {e}")
            return False
    
    def _generate_random_employee_data(self, _id: uuid.UUID) -> dict:

        """
        Generate a dictionary with randomized employee data, ensuring unique email addresses

        This method checks for existing employees with the same first name and surname,
        and appends a numerical suffix to the email if necessary to avoid duplicates

        Args:
            _id (uuid.UUID): A unique identifier for the employee

        Returns:
            dict: A dictionary containing the following employee information:
                - "id" (uuid.UUID): The unique identifier of the employee
                - "nombre" (str): The employee's first name
                - "apellido" (str): The employee's surname
                - "departamento" (str): The employee's department number (randomly chosen between 1 and 10)
                - "sueldo" (str): The employee's salary (formatted as a string with 2 decimal places)
                - "fecha" (str): The employee's birth date in "YYYY-MM-DD" format
                - "email" (str): The employee's unique email address
        
        Raises:
            logging.error: If the employee ID already exists in the database
        """

        if _id in self.employees:
            logging.error(f"Error generating employee data: ID {_id} already exists.")
            return {}
        
        _name = names.get_first_name()
        _surname = names.get_last_name()
        _department = random.randint(1, 10)
        _salary = random.randint(10000, 20000)
        _year = random.randint(dt.now().year-65, dt.now().year-18)
        _month = random.randint(1, 12)
        _, _days_in_month = calendar.monthrange(_year, _month)
        _day = random.randint(1, _days_in_month)
        _date = dt(_year, _month, _day).strftime("%Y-%m-%d")

        _existing_emails = [employee['email'] for employee in self.employees.values() if employee['nombre'].lower() == _name.lower() and employee['apellido'].lower() == _surname.lower()]
        if not _existing_emails:
            _email = f"{_name}.{_surname}@{self.EMAIL_SUFFIX}.com"
        else:
            # Ensures no duplicates even after deletions
            existing_suffixes = [int(email.split(_surname)[-1].split('@')[0]) for email in _existing_emails if email.split(_surname)[-1].split('@')[0].isdigit()]
            highest_suffix = max(existing_suffixes) if existing_suffixes else 0
            _email = f"{_name}.{_surname}{highest_suffix + 1}@{self.EMAIL_SUFFIX}.com"

        return {
            "id": _id,
            "nombre": _name,
            "apellido": _surname,
            "departamento": str(_department),
            "sueldo": f"{_salary:.2f}",
            "fecha": _date,
            "email": _email.lower()
        }
    
    def get_employees(self) -> dict:
        """
        Retrieve all employees from the database

        Returns:
            dict: A dictionary containing all employees in the database
        """
        return self.employees
    
    def add_employee_with_random_data(self) -> bool:

        """
        Add a new employee with randomly generated data to the database

        Returns:
            bool: True if the employee is added successfully, False otherwise
        """

        if len(self.employees) >= self.MAX_EMPLOYEES:
            logging.warning("Maximum number of employees reached.")
            return False
        
        _id = uuid.uuid4()
        _employee = self._generate_random_employee_data(_id)
        self.employees[_id] = _employee

        result = self._update_file()         
        if not result:
            logging.error(f"Error adding new employee.")
            self._load_data() # Revert dictionary to file contents
            return False
        
        logging.info(f"Employee with ID {_id} added correctly.")
        return True
    
    def remove_employee(self, id_: uuid.UUID) -> bool:
        """
        Remove an employee from the database by their ID

        Args:
            id_ (uuid.UUID): The unique identifier of the employee to be removed

        Returns:
            bool: True if the employee was removed successfully, False otherwise
        """
        if len(self.employees) == 0:
            print("Employee database is empty")
            return False
        
        if id_ in self.employees:
            del self.employees[id_]
            result = self._update_file()         
            if not result:
                logging.error(f"Failed to remove employee.")
                self._load_data()
                return False
            
            logging.info(f"Employee with ID {id_} removed correctly.")
            return True
        else:
            logging.error(f"Error removing employee: ID {id_} not found.")
            return False
    
    def reset_employees(self) -> bool:
        """
        Reset the employee database to an empty state

        Returns:
            bool: True if the reset was successful, False otherwise
        """
        self.employees = {}
        result = self._update_file()
        if not result:
            logging.error("Error resetting employee database.")
            self._load_data()
            return False

        logging.info("Employee database reset successfully.")
        return True
    
    def employee_exists(self, id_: uuid.UUID) -> bool:
        """
        Check if an employee exists in the database by their ID

        Args:
            id_ (uuid.UUID): The unique identifier of the employee

        Returns:
            bool: True if the employee exists, False otherwise
        """
        return id_ in self.employees
    
    @property
    def read_only_fields(self) -> list:
        return ['id', 'fecha', 'email']
    
    def modify_employee_field(self, id_: uuid.UUID, field_name: str, field_value: str) -> bool:
        """
        Modify a specific field of an employee in the database

        Args:
            id_ (uuid.UUID): The unique identifier of the employee
            field_name (str): The name of the field to be updated
            field_value (str): The new value to set for the specified field

        Returns:
            bool: True if the field was updated successfully, False otherwise
        """
        if field_name in self.read_only_fields:
            logging.error("Field is read-only")
            return False
        
        if id_ in self.employees:
            if field_name in self.employees[id_]:
                self.employees[id_][field_name] = field_value
                
                result = self._update_file()
                if not result:
                    logging.error("Error updating field.")
                    self._load_data()
                    return False
                
                logging.info(f"Field '{field_name}' in ID {id_} updated correctly.")
                return True
            else:
                logging.error(f"Error updating field: Invalid field name '{field_name}'.")
                return False
        else:
            logging.error(f"Error updating field: ID {id_} not found.")
            return False
        
    def get_employee(self, id_: uuid.UUID) -> dict:
        """
        Retrieve an employee's data from the database by their ID

        Args:
            id_ (uuid.UUID): The unique identifier of the employee

        Returns:
            dict: The employee's data if found, None otherwise
        """
        if id_ in self.employees:
            return self.employees[id_]
        else:
            logging.warning(f"ID {id_} not found.")
            return None
    
    def get_by_field(self, field_name: str, operator_: str, field_value: str) -> dict:
        """
        Retrieve a dictionary of employees that match a specified field and comparison criteria

        Args:
            field_name (str): The name of the field to filter employees by
            operator (str): The comparison operator ('==', '!=', '<', '<=', '>', or '>=')
            field_value: The value to compare against

        Returns:
            dict: A dictionary of employees matching the criteria.
        """
        operators = {
            '==': operator.eq,
            '!=': operator.ne,
            '<': operator.lt,
            '<=': operator.le,
            '>': operator.gt,
            '>=': operator.ge
        }
        
        if operator_ not in operators:
            logging.error(f"Invalid operator '{operator}'")
            return {}
        
        if not any(field_name in employee for employee in self.employees.values()):
            logging.error(f"Field '{field_name}' does not exist.")
            return {}
        
        converted_field_value = self._cast_str(field_value)
        comparison_function = operators[operator_]
        
        matching_employees = {
            id_: employee
            for id_, employee in self.employees.items()
            if comparison_function(self._cast_str(employee[field_name]), converted_field_value)
        }
        
        return matching_employees
    
    def _cast_str(self, value: str) -> any:
        """
        Detect and convert the input value to the appropriate type

        Args:
            value (str): The input value to be converted

        Returns:
            The converted value in its appropriate type or the original value if conversion fails
        """
        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        if value.lower() in ['true', 'false']:
            return value.lower() == 'true'

        return value